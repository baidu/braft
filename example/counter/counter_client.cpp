/*
 * =====================================================================================
 *
 *       Filename:  counter_client.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年10月30日 15时14分10秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <stdint.h>
#include <gflags/gflags.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "counter.pb.h"
#include "raft/raft.h"
#include "raft/util.h"
#include "cli.h"

#include <pb_to_json.h>

DEFINE_string(peers, "", "current cluster peer set");
DEFINE_string(shutdown, "", "shutdown peer");
DEFINE_string(snapshot, "", "snapshot peer");
DEFINE_string(stats, "", "stats peer");
DEFINE_string(setpeer, "", "setpeer peer");
DEFINE_string(new_peers, "", "new cluster peer set");
DEFINE_int32(threads, 1, "work threads");
DEFINE_int64(fetch_and_add_num, 0, "fetch_and_add num per thread, 0 disable, -1 always");

class CounterClient : public example::CommonCli {
public:
    CounterClient(const std::vector<raft::PeerId>& peers)
        : example::CommonCli(peers) {
    }
    virtual ~CounterClient() {
    }

    int fetch_and_add() {
        // TODO check limit
        int64_t* fetch_and_add_count = base::get_thread_local<int64_t>();
        if (_fetch_and_add_limit > 0 && (*fetch_and_add_count)++ >= _fetch_and_add_limit) {
            LOG(WARNING) << "reach limit " << _fetch_and_add_limit;
            return -1;
        }

        base::EndPoint leader_addr = get_leader_addr();
        //TODO: timeout? retry?
        while (true) {
            baidu::rpc::ChannelOptions channel_opt;
            channel_opt.timeout_ms = -1;
            baidu::rpc::Channel channel;

            if (channel.Init(leader_addr, &channel_opt) != 0) {
                LOG(ERROR) << "channel init failed, " << leader_addr;
                leader_addr = get_leader_addr();
                sleep(1);
                continue;
            }
            baidu::rpc::Controller cntl;
            counter::CounterService_Stub stub(&channel);
            counter::FetchAndAddRequest request;
            request.set_ip(0); // fake, server get from controller::remote_side
            request.set_pid(getpid());
            request.set_req_id(base::fast_rand());
            request.set_value(1);
            counter::FetchAndAddResponse response;
            stub.fetch_and_add(&cntl, &request, &response, NULL);
            if (cntl.Failed()) {
                LOG(WARNING) << "fetch_and_add error: " << cntl.ErrorText();
                leader_addr = get_leader_addr();
                sleep(1);
                continue;
            }

            if (!response.success()) {
                LOG(WARNING) << "fetch_and_add failed, redirect to " << response.leader();
                base::str2endpoint(response.leader().c_str(), &leader_addr);
                continue;
            } else {
                LOG(NOTICE) << "fetch_and_add success to " << leader_addr
                    << " index: " << response.index() << " ret: " << response.value();
                set_leader_addr(leader_addr);
                break;
            }
        }

        return 0;
    }

    static void* run_counter_thread(void* arg) {
        CounterClient* client = (CounterClient*)arg;

        // init tls, tls new not memset 0
        int64_t* fetch_and_add_count = base::get_thread_local<int64_t>();
        *fetch_and_add_count = 0;
        base::EndPoint* tls_addr = base::get_thread_local<base::EndPoint>();
        tls_addr->ip = base::IP_ANY;
        tls_addr->port = 0;

        // run
        while (true) {
            if (0 != client->fetch_and_add()) {
                break;
            }
        }

        return NULL;
    }

    int start(int threads, int64_t fetch_and_add_num) {
        _fetch_and_add_limit = fetch_and_add_num;

        CHECK(threads >= 1);
        for (int i = 0; i < threads; i++) {
            pthread_t tid;
            pthread_create(&tid, NULL, run_counter_thread, this);

            _threads.push_back(tid);
        }
        return 0;
    }

    int join() {
        for (size_t i = 0; i < _threads.size(); i++) {
            pthread_join(_threads[i], NULL);
        }
        return 0;
    }
private:
    base::EndPoint get_leader_addr() {
        base::EndPoint* tls_addr = base::get_thread_local<base::EndPoint>();
        if (tls_addr->ip != base::IP_ANY) {
            CHECK(tls_addr->port != 0);
            return *tls_addr;
        } else {
            int index = base::fast_rand_less_than(_peers.size());
            return _peers[index].addr;
        }
    }

    void set_leader_addr(const base::EndPoint& addr) {
        base::EndPoint* tls_addr = base::get_thread_local<base::EndPoint>();
        *tls_addr = addr;
    }

    int64_t _fetch_and_add_limit;
    std::vector<pthread_t> _threads;
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // stats
    if (!FLAGS_stats.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_stats.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_stats.c_str(), &addr)) {
            return CounterClient::stats(addr);
        } else {
            LOG(ERROR) << "stats flags bad format: " << FLAGS_stats;
            return -1;
        }
    }

    // snapshot
    if (!FLAGS_snapshot.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_snapshot.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_snapshot.c_str(), &addr)) {
            return CounterClient::snapshot(addr);
        } else {
            LOG(ERROR) << "snapshot flags bad format: " << FLAGS_snapshot;
            return -1;
        }
    }

    // shutdown
    if (!FLAGS_shutdown.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_shutdown.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_shutdown.c_str(), &addr)) {
            return CounterClient::shutdown(addr);
        } else {
            LOG(ERROR) << "shutdown flags bad format: " << FLAGS_shutdown;
            return -1;
        }
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }
    if (peers.size() == 0) {
        LOG(ERROR) << "need set peers flags";
        return -1;
    }

    std::vector<raft::PeerId> new_peers;
    if (!FLAGS_new_peers.empty()) {
        const char* the_string_to_split = FLAGS_new_peers.c_str();
        for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
            raft::PeerId peer(std::string(s.field(), s.length()));
            new_peers.push_back(peer);
        }
    }

    // set peer
    if (!FLAGS_setpeer.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_setpeer.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_setpeer.c_str(), &addr)) {
            return CounterClient::set_peer(addr, peers, new_peers);
        } else {
            LOG(ERROR) << "setpeer flags bad format: " << FLAGS_setpeer;
            return -1;
        }
    }

    CounterClient counter_client(peers);

    // add_peer/remove_peer
    if (!new_peers.empty()) {
        // add_peer/remove_peer
        return counter_client.set_peer(new_peers);
    }

    // fetch_and_add
    if (FLAGS_fetch_and_add_num != 0) {
        counter_client.start(FLAGS_threads, FLAGS_fetch_and_add_num);
        counter_client.join();
        return 0;
    }

    return 0;
}

