/*
 * =====================================================================================
 *
 *       Filename:  client_req_id.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/17 19:34:48
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */
#ifndef PUBLIC_RAFT_EXAMPLE_COUNTER_CLIENT_REQ_ID_H
#define PUBLIC_RAFT_EXAMPLE_COUNTER_CLIENT_REQ_ID_H

#include <stdint.h>
#include <base/containers/mru_cache.h>
#include <base/third_party/murmurhash3/murmurhash3.h>

namespace counter {

struct ClientRequestId {
    int32_t ip;
    int32_t pid;
    int64_t req_id;

    ClientRequestId(int32_t ip_, int32_t pid_, int64_t req_id_)
        : ip(ip_), pid(pid_), req_id(req_id_) {}

    bool operator <(const ClientRequestId& other) const {
        if (ip < other.ip) {
            return true;
        } else if (ip == other.ip && pid < other.pid) {
            return true;
        } else if (ip == other.ip && pid == other.pid && req_id < other.req_id) {
            return true;
        } else {
            return false;
        }
    }
    bool operator == (const ClientRequestId& other) const {
        return ip == other.ip && pid == other.pid && req_id == other.req_id;
    }
};

struct FetchAndAddResult {
    int64_t value;
    int64_t index;

    FetchAndAddResult(int64_t value_, int64_t index_) : value(value_), index(index_) {}
};

// done map
typedef base::HashingMRUCache<ClientRequestId, FetchAndAddResult> CounterDuplicatedRequestCache;

}

namespace BASE_HASH_NAMESPACE {
template <>
struct hash<counter::ClientRequestId> {
    std::size_t operator()(const counter::ClientRequestId& id) const {
        int64_t v1 = ((int64_t)id.ip << 32) | id.pid;
        v1 = base::fmix64(v1);
        int64_t v2 = id.req_id;
        v2 = base::fmix64(v2);
        return base::HashInts64(v1, v2);
    }
};

}

#endif //~PUBLIC_RAFT_EXAMPLE_COUNTER_CLIENT_REQ_ID_H
