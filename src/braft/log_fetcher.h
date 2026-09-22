// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Kai FAN(fankai@baidu.com)

#ifndef BRAFT_RAFT_LOG_FETCHER_H
#define BRAFT_RAFT_LOG_FETCHER_H

#include <brpc/channel.h>          // brpc::Channel
#include <butil/memory/ref_counted.h>

#include "braft/raft.h"

namespace braft {

class FetchLogClosure: public Closure {
public:
    FetchLogClosure(LearnerImpl* learner) : _learner(learner) {}

    ~FetchLogClosure() {
    }
    virtual void Run() override;
private:
    LearnerImpl* _learner;
};

class LogFetcher : public butil::RefCountedThreadSafe<FileSystemAdaptor> {
    DISALLOW_COPY_AND_ASSIGN(LogFetcher);
public:
    LogFetcher(const GroupId& group_id, const PeerId &peer, LearnerImpl* learner, 
               const int64_t last_log_index, const int fetch_log_timeout_s);
    virtual ~LogFetcher();

    int init();
    void fetch_log();

private:
    int64_t _next_index;
    GroupId _group_id;
    PeerId _peer_id;
    brpc::Channel _channel;
    LearnerImpl* _learner;
    int _fetch_log_timeout_s;
    int64_t _expire_time_s;
};

}

#endif
