// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Kai FAN(fankai@baidu.com)

#include "braft/log_fetcher.h"
#include "braft/learner.h"
#include "braft/log_manager.h"

namespace braft {

DEFINE_int32(raft_max_fetch_log_size, 200000, "fetch log limit per cycle");

void FetchLogClosure::Run() {
    _learner->on_fetch_log_error();
    delete this;
}

LogFetcher::LogFetcher(const GroupId& group_id, const PeerId& peer_id, LearnerImpl* learner, 
                       const int64_t last_log_index, const int fetch_log_timeout_s)
        : _group_id(group_id)
        , _peer_id(peer_id)
        , _learner(learner)
        , _fetch_log_timeout_s(fetch_log_timeout_s) {
    _next_index = last_log_index + 1;
    _expire_time_s = butil::timespec_to_seconds(butil::seconds_from_now(_fetch_log_timeout_s));
    _learner->AddRef();
}

LogFetcher::~LogFetcher() {
    _learner->Release();
    _learner = NULL;
}

int LogFetcher::init() {
    brpc::ChannelOptions channel_option;
    if (_channel.Init(_peer_id.addr, &channel_option) != 0) {
        LOG(ERROR) << "Fail to initialize [" << _peer_id << "]";
        return -1;
    }
    return 0;
}

void LogFetcher::fetch_log() {
    int64_t last_committed_index = _learner->last_committed_index();
    CHECK(_next_index >= last_committed_index);
    int64_t uncommit_size = _next_index - last_committed_index;
    int max_size = FLAGS_raft_max_fetch_log_size > uncommit_size ?
            (FLAGS_raft_max_fetch_log_size - uncommit_size) : 0;
    if (max_size <= 0) {
        LOG(INFO) << "Skip fetch log round, too many uncommit logs, uncommit_size:"
                   << uncommit_size;
        return;
    }

    int64_t now = butil::gettimeofday_s();
    if (_expire_time_s <= now) {
        // TODO : session id
        BRAFT_VLOG << "Fail to fetch log, session expired";
        FetchLogClosure* done = new FetchLogClosure(_learner);
        run_closure_in_bthread(done);
        return;
    }

    brpc::Controller cntl;
    ReadCommittedLogsRequest request;
    request.set_group_id(_group_id);
    request.set_peer_id(_peer_id.to_string());
    request.set_first_index(_next_index);
    request.set_count(max_size);
    ReadCommittedLogsResponse response;
    RaftService_Stub stub(&_channel);
    cntl.set_timeout_ms(FLAGS_raft_learner_req_timeout_ms);
    stub.read_committed_logs(&cntl, &request, &response, NULL);
    if (cntl.Failed() || !response.success()) {
        LOG(INFO) << "Fail to fetch log, rpc failed, errmsg:" << cntl.ErrorText();
        FetchLogClosure* done = new FetchLogClosure(_learner);
        run_closure_in_bthread(done);
        return;
    }

    std::vector<LogEntry*> entries;
    butil::IOBuf& data_buf = cntl.response_attachment();
    entries.reserve(response.entries_size());
    for (int i = 0; i < response.entries_size(); ++i) {
        const EntryMeta& meta = response.entries(i);
        LogEntry* entry = new LogEntry;
        entry->AddRef();
        entry->type = meta.type();
        entry->id = LogId(_next_index + i, meta.term());
        if (meta.peers_size() > 0) {
            entry->peers = new std::vector<PeerId>;
            for (int i = 0; i < meta.peers_size(); ++i) {
                entry->peers->push_back(meta.peers(i));
            }
        }
        if (meta.old_peers_size() > 0) {
            entry->old_peers = new std::vector<PeerId>;
            for (int i = 0; i < meta.old_peers_size(); ++i) {
                entry->old_peers->push_back(meta.old_peers(i));
            }
        }
        data_buf.cutn(&entry->data, meta.data_len());
        entries.push_back(entry);
        BRAFT_VLOG << "fetch log " << entry->id.index << ", length:" << meta.data_len();
    }

    if (entries.empty()) {
        return;
    }
    int count = _learner->append_entries(&entries);
    if (count < 0) {
        for (size_t i = 0; i < entries.size(); ++i) {
            entries[i]->Release();
        }
        entries.clear();
        FetchLogClosure* done = new FetchLogClosure(_learner);
        run_closure_in_bthread(done);
        return;
    }

    _next_index += count;
    _expire_time_s = butil::timespec_to_seconds(butil::seconds_from_now(_fetch_log_timeout_s));
    LOG(INFO) << "Fetched logs from " << _peer_id << ", start_index:" << request.first_index() 
               << ", max_size:" << max_size << ", count:" << count;
}

} // raft
