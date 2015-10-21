/*
 * =====================================================================================
 *
 *       Filename:  node.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/10/08 17:00:15
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include "raft/util.h"
#include "raft/raft.h"
#include "raft/node.h"
#include <bthread_unstable.h>

#include "baidu/rpc/errno.pb.h"
#include "baidu/rpc/controller.h"
#include "baidu/rpc/channel.h"

namespace raft {

//int LogEntryCommitmentWaiter::on_committed(int64_t last_commited_index, void *context) {
//    LOG(TRACE) << "node " << _id << " commit " << last_commited_index;
//
//    //TODO: create bthread?
//    base::Closure* done = static_cast<base::Closure*>(context);
//    if (done) {
//        //TODO:
//        done->Run();
//    }
//    return 0;
//}
//
//int LogEntryCommitmentWaiter::on_cleared(int64_t log_index, void *context, int error_code) {
//    LOG(TRACE) << "node " << _id << " clear " << log_index << ", maybe stepdown";
//    base::Closure* done = static_cast<base::Closure*>(context);
//    if (done) {
//        //TODO:
//        delete done;
//    }
//    return 0;
//}

NodeImpl::NodeImpl(const GroupId& group_id, const PeerId& server_id, const NodeOptions* options)
    : Node(group_id, server_id, options),
    _group_id(group_id), _server_id(server_id),
    _state(FOLLOWER), _current_term(0),
    _last_snapshot_term(0), _last_snapshot_index(0),
    _last_leader_timestamp(base::monotonic_time_ms()),
    _ref_count(0) {

        add_ref();
        if (options) {
            _options = *options;
        }
        bthread_mutex_init(&_mutex, NULL);
}

NodeImpl::~NodeImpl() {
    bthread_mutex_destroy(&_mutex);
}

int NodeImpl::apply(const void* data, const int len, base::Closure* done) {
    if (_state != LEADER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " can't apply not in LEADER";
        return baidu::rpc::SYS_EINVAL;
    }

    LogEntry* entry = new LogEntry;
    entry->term = _current_term;
    entry->type = ENTRY_TYPE_DATA;
    entry->len = len;
    entry->data = (void*)data; //FIXME
    return append(entry, done);
}

int NodeImpl::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                       base::Closure* done) {
    if (_state != LEADER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " can't apply not in LEADER";
        return baidu::rpc::SYS_EINVAL;
    }
    return 0;
}

int NodeImpl::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                          base::Closure* done) {
    if (_state != LEADER) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id << " can't apply not in LEADER";
        return baidu::rpc::SYS_EINVAL;
    }
    return 0;
}

int NodeImpl::set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers,
                 base::Closure* done) {
    return 0;
}

int NodeImpl::shutdown(base::Closure* done) {
    return 0;
}

static void on_election_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_election_timeout();
    node->release();
}

void NodeImpl::handle_election_timeout() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    // check state
    if (_state != FOLLOWER) {
        return;
    }
    // check timestamp
    if (base::monotonic_time_ms() - _last_leader_timestamp < _options.election_timeout) {
        add_ref();
        int64_t election_timeout = random_timeout(_options.election_timeout);
        bthread_timer_add(&_election_timer, base::milliseconds_from_now(election_timeout),
                          on_election_timer, this);
        LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " start elect_timer";
        return;
    }

    // first vote
    LOG(NOTICE) << "node " << _group_id << ":" << _server_id << " start elect";
    elect_self();
}

static void on_vote_timer(void* arg) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);

    node->handle_vote_timeout();
    node->release();
}

void NodeImpl::handle_vote_timeout() {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    // check state
    if (_state == CANDIDATE) {
        // retry vote
        LOG(NOTICE) << "node " << _group_id << ":" << _server_id << " retry elect";
        elect_self();
    }
}

void NodeImpl::handle_request_vote_response(const PeerId& peer_id, const int64_t term,
                                            const RequestVoteResponse& response) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    // check state
    if (_state != CANDIDATE) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid RequestVoteResponse from " << peer_id
            << " state not in CANDIDATE";
        return;
    }
    // check stale response
    if (term != _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received stale RequestVoteResponse from " << peer_id
            << " term " << term << " current_term " << _current_term;
        return;
    }
    // check response term
    if (response.term() > _current_term) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received invalid RequestVoteResponse from " << peer_id
            << " term " << response.term() << " expect " << _current_term;
        step_down(response.term());
        return;
    }

    LOG(NOTICE) << "node " << _group_id << ":" << _server_id
        << " received RequestVoteResponse from " << peer_id
        << " term " << response.term() << " granted " << response.granted();
    // check granted quorum?
    if (response.granted()) {
        _vote_ctx.grant(peer_id);
        if (_vote_ctx.quorum()) {
            become_leader();
        }
    }
}

struct OnRequestVoteRPCDone : public google::protobuf::Closure {
    OnRequestVoteRPCDone(const PeerId& peer_id_, const int64_t term_, NodeImpl* node_)
        : peer(peer_id_), term(term_), node(node_) {
            node->add_ref();
    }
    virtual ~OnRequestVoteRPCDone() {}
    void Run() {
        if (cntl.ErrorCode() != 0) {
            LOG(WARNING) << "node " << node->group_id() << ":" << node->server_id()
                << " RequestVote to " << peer << " error: " << cntl.ErrorText();
            return;
        }
        node->handle_request_vote_response(peer, term, response);
        node->release();
    }
    PeerId peer;
    int64_t term;
    RequestVoteResponse response;
    baidu::rpc::Controller cntl;
    NodeImpl* node;
};

// in lock
int64_t NodeImpl::last_log_term() {
    int64_t term = 0;
    int64_t last_log_index = _log_manager->last_log_index();
    if (last_log_index >= _log_manager->first_log_index()) {
        term = _log_manager->get_term(last_log_index);
    } else {
        term = _last_snapshot_term;
    }
    return term;
}

// in lock
void NodeImpl::elect_self() {
    // cancel follower election timer
    if (_state == FOLLOWER) {
        LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " stop elect_timer";
        int ret = bthread_timer_del(_election_timer);
        if (ret == 0) {
            release();
        } else {
            assert(ret == 1);
        }
    }
    _state = CANDIDATE;
    _current_term++;
    _voted_id = _server_id;
    _vote_ctx.reset();

    add_ref();
    int64_t vote_timeout = random_timeout(std::max(_options.election_timeout / 10, 1));
    bthread_timer_add(&_vote_timer, base::milliseconds_from_now(vote_timeout), on_vote_timer, this);
    LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " start vote_timer";

    for (size_t i = 0; i < _conf.peers.size(); i++) {
        baidu::rpc::ChannelOptions options;
        options.connection_type = baidu::rpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        baidu::rpc::Channel channel;
        if (0 != channel.Init(_conf.peers[i].addr, &options)) {
            LOG(WARNING) << "channel init failed, addr " << _conf.peers[i].addr;
            continue;
        }

        RequestVoteRequest request;
        request.set_group_id(_group_id);
        request.set_server_id(_server_id.to_string());
        request.set_peer_id(_conf.peers[i].to_string());
        request.set_term(_current_term);
        request.set_last_log_term(last_log_term());
        request.set_last_log_index(_log_manager->last_log_index());

        OnRequestVoteRPCDone* done = new OnRequestVoteRPCDone(_conf.peers[i], _current_term, this);
        RaftService_Stub stub(&channel);
        stub.request_vote(&done->cntl, &request, &done->response, done);
    }

    _vote_ctx.grant(_server_id);
    //TODO: outof lock
    _stable->set_term_and_votedfor(_current_term, _server_id);
    if (_vote_ctx.quorum()) {
        become_leader();
    }
}

// in lock
void NodeImpl::step_down(const int64_t term) {
    if (_state == CANDIDATE) {
        LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " stop vote_timer";
        int ret = bthread_timer_del(_vote_timer);
        if (0 == ret) {
            release();
        } else {
            assert(ret == 1);
        }
    } else {
        assert(_state == LEADER);
        _commit_manager->clear_pending_applications();
    }

    _state = FOLLOWER;
    _leader_id.reset();
    _current_term = term;
    _voted_id.reset();
    //TODO: outof lock
    _stable->set_term_and_votedfor(term, _voted_id);

    // start election timer
    add_ref();
    int64_t election_timeout = random_timeout(_options.election_timeout);
    bthread_timer_add(&_election_timer, base::milliseconds_from_now(election_timeout),
                      on_election_timer, this);
    LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " start election_timer";
    // TODO: stop stagging new node
    // TODO: wait disk thread empty
}

// in lock
void NodeImpl::become_leader() {
    assert(_state == CANDIDATE);
    // cancel candidate vote timer
    LOG(DEBUG) << "node " << _group_id << ":" << _server_id << " stop vote_timer";
    bthread_timer_del(_vote_timer);
    release();

    _state = LEADER;
    _leader_id = _server_id;

    //TODO: add lease timer to check contact timestamp to step down
    //bthread_timer_add(&_lease_timer, base::milliseconds_from_now(_options.election_timeout),
    //                  on_election_timer, this);

    _commit_manager->reset_pending_index(_log_manager->last_log_index() + 1);

    LogEntry* entry = new LogEntry;
    entry->term = _current_term;
    entry->type = ENTRY_TYPE_NO_OP;

    append(entry, NULL);
}

static int on_leader_stable(void* arg, int64_t log_index, int error_code) {
    NodeImpl* node = static_cast<NodeImpl*>(arg);
    //TODO
    int ret = 0;
    if (error_code == 0) {
        ret = node->advance_commit_index(PeerId(), log_index);
    }
    node->release();
    return ret;
}

int NodeImpl::advance_commit_index(const PeerId& peer_id, const int64_t log_index) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    if (peer_id.is_empty()) {
        // leader thread
        _commit_manager->set_stable_at_peer_reentrant(log_index, _server_id);
    } else {
        // peer thread
        _commit_manager->set_stable_at_peer_reentrant(log_index, peer_id);
    }
    return 0;
}

int NodeImpl::append(LogEntry* entry, base::Closure* done) {
    _commit_manager->append_pending_application(_conf, done);
    add_ref();
    _log_manager->append(entry, on_leader_stable, this);
    return 0;
}

int NodeImpl::handle_request_vote_request(const RequestVoteRequest* request,
                                          RequestVoteResponse* response) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    //TODO: leader call _log_manager->xxx() after stepdown() ?
    int64_t last_log_index = _log_manager->last_log_index();
    int64_t last_log_term = this->last_log_term();
    bool log_is_ok = (request->last_log_term() > last_log_term ||
                      (request->last_log_term() == last_log_term &&
                       request->last_log_index() >= last_log_index));
    PeerId candidate_id;
    if (0 != candidate_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received RequestVote from " << request->server_id()
            << " server_id bad format";
        return baidu::rpc::SYS_EINVAL;
    }

    do {
        // check leader to tolerate network partitioning:
        //     1. leader always reject RequstVote
        //     2. follower reject RequestVote before change to candidate
        if (!_leader_id.is_empty()) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " reject RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term
                << " current_leader " << _leader_id;
            break;
        }

        // check term
        if (request->term() >= _current_term) {
            LOG(NOTICE) << "node " << _group_id << ":" << _server_id
                << " received RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term: " << _current_term;
            // incress current term, change state to follower
            if (request->term() > _current_term) {
                step_down(request->term());
            }
        } else {
            // ignore older term
            LOG(NOTICE) << "node " << _group_id << ":" << _server_id
                << " ignore RequestVote from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        // save
        if (log_is_ok && _voted_id.is_empty()) {
            _voted_id = candidate_id;
            //TODO: outof lock
            _stable->set_votedfor(candidate_id);
        }
    } while (0);

    response->set_term(_current_term);
    response->set_granted(request->term() == _current_term && _voted_id == candidate_id);
    return 0;
}

int NodeImpl::handle_append_entries_request(base::IOBuf& data_buf,
                                            const AppendEntriesRequest* request,
                                            AppendEntriesResponse* response) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    PeerId server_id;
    if (0 != server_id.parse(request->server_id())) {
        LOG(WARNING) << "node " << _group_id << ":" << _server_id
            << " received RequestVote from " << request->server_id()
            << " server_id bad format";
        return baidu::rpc::SYS_EINVAL;
    }

    bool success = false;
    do {
        // check stale term
        if (request->term() < _current_term) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " ignore stale AppendEntries from " << request->server_id()
                << " in term " << request->term()
                << " current_term " << _current_term;
            break;
        }

        // check term and state to step down
        if (request->term() > _current_term || _state != FOLLOWER) {
            step_down(request->term());
        }

        // save current leader
        if (_leader_id.is_empty()) {
            _leader_id = server_id;
        }

        // check prev log gap
        if (request->prev_log_index() > _log_manager->last_log_index()) {
            LOG(WARNING) << "node " << _group_id << ":" << _server_id
                << " reject index_gapped AppendEntries from " << request->server_id()
                << " in term " << request->term()
                << " prev_log_index " << request->prev_log_index()
                << " last_log_index " << _log_manager->last_log_index();
            break;
        }

        // check prev log term
        if (request->prev_log_index() >= _log_manager->first_log_index()) {
            int64_t local_term = _log_manager->get_term(request->prev_log_index());
            if (local_term != request->prev_log_term()) {
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                    << " reject term_unmatched AppendEntries from " << request->server_id()
                    << " in term " << request->term()
                    << " prev_log_index " << request->prev_log_index()
                    << " prev_log_term " << request->prev_log_term()
                    << " prev_log_term_local " << local_term;
                break;
            }
        }

        success = true;

        std::vector<LogEntry*> entries;
        int64_t index = request->prev_log_index();
        for (int i = 0; i < request->entries_size(); i++) {
            index++;

            const Entry& entry = request->entries(i);

            if (index < _log_manager->first_log_index()) {
                // log maybe discard after snapshot, skip retry AppendEntries rpc
                continue;
            }
            // mostly index == _log_manager->last_log_index() + 1
            if (_log_manager->last_log_index() >= index) {
                if (_log_manager->get_term(index) == entry.term()) {
                    // check same index entry's term when duplicated rpc
                    continue;
                }
                
                int64_t last_index_kept = index - 1;
                LOG(WARNING) << "node " << _group_id << ":" << _server_id
                    << " truncate from " << _log_manager->last_log_index()
                    << " to " << last_index_kept;

                _log_manager->truncate_suffix(last_index_kept);
                //TODO: truncate configuration
            }

            if (entry.type() != ENTRY_TYPE_UNKNOWN) {
                LogEntry* log_entry = new LogEntry();
                log_entry->term = entry.term();
                log_entry->type = (EntryType)entry.type();
                if (entry.peers_size() > 0) {
                    //log_entry->peers = new std::vector<PeerId>;
                    log_entry->peers = new std::vector<std::string>;
                    for (int i = 0; i < entry.peers_size(); i++) {
                        //TODO: move std::string to PeerId
                        //peers->push_back(PeerId(entry.peers(i)));
                        log_entry->peers->push_back(entry.peers(i));
                    }
                    assert((log_entry->type == ADD_PEER || log_entry->type == REMOVE_PEER));
                }

                if (entry.has_data_len()) {
                    int len = entry.data_len();
                    char* data = (char*)malloc(len);
                    data_buf.cutn(data, len);
                    log_entry->len = len;
                    log_entry->data = data;
                }

                entries.push_back(log_entry);
            }
        }
        //TODO: outof lock
        int ret = _log_manager->append_entries(entries);
    } while (0);

    response->set_term(_current_term);
    response->set_success(success);
    if (success) {
        //TODO:
        response->set_last_log_index(_log_manager->last_log_index());
        _commit_manager->set_last_committed_index(request->committed_index());
        _last_leader_timestamp = base::monotonic_time_ms();
    }
    return 0;
}

int NodeImpl::handle_install_snapshot_request(const InstallSnapshotRequest* request,
                                              InstallSnapshotResponse* response) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);

    return 0;
}

NodeManager::NodeManager() {
    bthread_mutex_init(&_mutex, NULL);
}

NodeManager::~NodeManager() {
    bthread_mutex_destroy(&_mutex);
}

Node* NodeManager::create(const GroupId& group_id, const PeerId& peer_id,
                          const NodeOptions* option) {
    Node* node = NULL;
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    NodeId node_id(group_id, peer_id);
    NodeMap::iterator it = _nodes.find(node_id);
    if (it == _nodes.end()) {
        node = new Node(group_id, peer_id, option);
        _nodes.insert(std::pair<NodeId, Node*>(node_id, node));
    } else {
        LOG(WARNING) << "node exist " << peer_id;
    }
    return node;
}

int NodeManager::destroy(Node* node) {
    {
        std::lock_guard<bthread_mutex_t> guard(_mutex);
        _nodes.erase(node->node_id());
    }
    //TODO: call node->shutdown()
    delete node;
    //node->release();
    return 0;
}

Node* NodeManager::get(const GroupId& group_id, const PeerId& peer_id) {
    std::lock_guard<bthread_mutex_t> guard(_mutex);
    NodeMap::iterator it = _nodes.find(NodeId(group_id, peer_id));
    if (it != _nodes.end()) {
        return it->second;
    } else {
        return NULL;
    }
}

}
