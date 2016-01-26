/*
 * =====================================================================================
 *
 *       Filename:  block_client.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015/11/27 15:21:34
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  WangYao (fisherman), wangyao02@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <stdint.h>
#include <algorithm>
#include <gflags/gflags.h>
#include <base/comlog_sink.h>
#include <base/string_splitter.h>
#include <baidu/rpc/channel.h>
#include "block.pb.h"
#include "raft/util.h"
#include "cli.h"

static const int64_t GB = 1024*1024*1024;
DEFINE_string(peers, "", "current cluster peer set");
DEFINE_string(shutdown, "", "shutdown peer");
DEFINE_string(snapshot, "", "snapshot peer");
DEFINE_string(setpeer, "", "setpeer peer");
DEFINE_string(new_peers, "", "new cluster peer set");

DEFINE_int32(threads, 1, "io threads");
DEFINE_int32(write_percent, 100, "write percent");
DEFINE_int64(rw_num, 0, "read/write num per threads");
DEFINE_string(local_path, "", "local block path");

class BlockClient : public example::CommonCli {
public:
    BlockClient(const std::vector<raft::PeerId>& peers)
        : example::CommonCli(peers),
        _write_percent(100), _rw_limit(-1), _fd(-1) {
            bthread_mutex_init(&_mutex, NULL);
    }
    virtual ~BlockClient() {
        bthread_mutex_destroy(&_mutex);
    }

    int write(int64_t start, int64_t end) {
        CHECK_LT(start, end);
        int64_t offset = base::fast_rand_in(start, end - 1);
        int64_t size = std::min(end - offset, 4*1024L);

        // write remote 
        block::WriteRequest request;
        request.set_offset(offset);
        request.set_size(static_cast<int32_t>(size));
        block::WriteResponse response;
        base::IOBuf request_data;
        for (int64_t i = 0; i < size; i++) {
            char c = 'a' + base::fast_rand_less_than(26);
            request_data.push_back(c);
        }

        base::EndPoint leader_addr = get_leader_addr();
        //TODO: timeout? retry?
        while (true) {
            baidu::rpc::Channel channel;
            if (channel.Init(leader_addr, NULL) != 0) {
                LOG(ERROR) << "channel init failed, " << leader_addr;
                leader_addr = get_leader_addr();
                sleep(1);
                continue;
            }

            block::BlockService_Stub stub(&channel);
            baidu::rpc::Controller controller;
            base::IOBuf& request_attachment = controller.request_attachment();
            request_attachment.append(request_data);
            stub.write(&controller, &request, &response, NULL);
            if (controller.Failed()) {
                LOG(ERROR) << "write failed, error: " << controller.ErrorText();
                leader_addr = get_leader_addr();
                //sleep(1);
                continue;
            }

            if (!response.success()) {
                LOG(WARNING) << "write failed, redirect to " << response.leader();
                base::str2endpoint(response.leader().c_str(), &leader_addr);
                continue;
            } else {
                //LOG(NOTICE) << "write success, offset: " << offset << ", size: " << size;
                set_leader_addr(leader_addr);
                break;
            }
        }

        // write local path
        if (_fd >= 0) {
            ssize_t nwriten = raft::file_pwrite(request_data, _fd, offset);
            CHECK(static_cast<size_t>(nwriten) == request_data.size())
                << "write failed, err: " << berror() << " offset: " << offset;
        }
        return 0;
    }

    int read(int64_t start, int64_t end) {
        CHECK_LT(start, end);
        int64_t offset = base::fast_rand_in(start, end - 1);
        int64_t size = std::min(end - offset, 4*1024L);

        block::ReadRequest request;
        request.set_offset(offset);
        request.set_size(static_cast<int32_t>(size));
        block::ReadResponse response;

        base::IOBuf response_data;
        base::EndPoint leader_addr = get_leader_addr();

        //TODO: timeout? retry?
        while (true) {
            baidu::rpc::Channel channel;
            if (channel.Init(leader_addr, NULL) != 0) {
                LOG(ERROR) << "channel init failed, " << leader_addr;
                leader_addr = get_leader_addr();
                sleep(1);
                continue;
            }

            block::BlockService_Stub stub(&channel);
            baidu::rpc::Controller controller;
            stub.read(&controller, &request, &response, NULL);
            if (controller.Failed()) {
                LOG(ERROR) << "read failed, error: " << controller.ErrorText();
                leader_addr = get_leader_addr();
                sleep(1);
                continue;
            }

            if (!response.success()) {
                LOG(WARNING) << "read failed, redirect to " << response.leader();
                base::str2endpoint(response.leader().c_str(), &leader_addr);
                sleep(1);
                continue;
            } else {
                LOG(NOTICE) << "read success, offset: " << offset << ", size: " << size
                    << ", ret: " << controller.response_attachment().size();
                set_leader_addr(leader_addr);
                response_data.append(controller.response_attachment());
                break;
            }
        }

        // check local
        if (_fd >= 0) {
            base::IOPortal portal;
            //read_at_offset(&portal, _fd, offset, size);
            ssize_t nread = raft::file_pread(&portal, _fd, offset, size);
            CHECK(nread >= 0) << "read failed, err: " << berror()
                << " fd: " << _fd << " offset: " << offset << " size: " << size;

            // check len
            CHECK_EQ(portal.size(), response_data.size())
                << "local size: " << portal.size() << "response size: " << response_data.size();
            // check hash
            CHECK(raft::murmurhash32(portal) == raft::murmurhash32(response_data))
                << "checksum unmatch, offset: " << offset << " size: " << size;
        }

        return 0;
    }

    int io(int64_t start, int64_t end) {
        // TODO check limit
        int64_t* rw_count = base::get_thread_local<int64_t>();
        if (_rw_limit > 0 && (*rw_count)++ >= _rw_limit) {
            LOG(WARNING) << "reach limit " << _rw_limit;
            return -1;
        }
        int percent = base::fast_rand_less_than(100);
        if (percent <= _write_percent) {
            return write(start, end);
        } else {
            return read(start, end);
        }
    }

    struct BlockThreadArgument {
        BlockClient* client;
        int64_t start;
        int64_t end;
    };

    static void* run_block_thread(void* arg) {
        BlockThreadArgument* thread_arg = (BlockThreadArgument*)arg;

        BlockClient* client = thread_arg->client;
        int64_t start = thread_arg->start;
        int64_t end = thread_arg->end;

        // init tls, tls new not memset 0
        int64_t* rw_count = base::get_thread_local<int64_t>();
        *rw_count = 0;
        base::EndPoint* tls_addr = base::get_thread_local<base::EndPoint>();
        tls_addr->ip = base::IP_ANY;
        tls_addr->port = 0;

        // run
        while (true) {
            if (0 != client->io(start, end)) {
                break;
            }
        }

        delete thread_arg;
        return NULL;
    }

    int start(int threads, int write_percent, int64_t rw_num,
              const std::string& local_path) {
        if (!local_path.empty()) {
            _fd = ::open(local_path.c_str(), O_CREAT | O_RDWR, 0644);
            CHECK(_fd >= 0) << "open local block failed, path: " << local_path
                << " , error: " << berror();
        }
        _write_percent = write_percent;
        _rw_limit = rw_num;

        threads = 1 << (int)log2(double(threads));
        CHECK(threads >= 1);
        for (int i = 0; i < threads; i++) {
            bthread_t tid;
            int64_t unit_size = GB / threads;
            BlockThreadArgument* arg = new BlockThreadArgument;
            arg->client = this;
            arg->start = i * unit_size;
            arg->end = (i + 1) * unit_size;
            LOG(NOTICE) << "thread " << i << " start: " << arg->start << " end: " << arg->end;
            bthread_start_background(&tid, NULL, run_block_thread, arg);

            _threads.push_back(tid);
        }
        return 0;
    }

    int join() {
        for (size_t i = 0; i < _threads.size(); i++) {
            bthread_join(_threads[i], NULL);
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

    bthread_mutex_t _mutex;
    int _write_percent;
    int64_t _rw_limit;
    std::vector<bthread_t> _threads;
    int _fd;
};

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    options.async = true;
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

    // snapshot
    if (!FLAGS_snapshot.empty()) {
        base::EndPoint addr;
        if (0 == base::hostname2endpoint(FLAGS_snapshot.c_str(), &addr) ||
            0 == base::str2endpoint(FLAGS_snapshot.c_str(), &addr)) {
            return BlockClient::snapshot(addr);
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
            return BlockClient::shutdown(addr);
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
            return BlockClient::set_peer(addr, peers, new_peers);
        } else {
            LOG(ERROR) << "setpeer flags bad format: " << FLAGS_setpeer;
            return -1;
        }
    }

    BlockClient block_client(peers);

    // add_peer/remove_peer
    if (!new_peers.empty()) {
        // add_peer/remove_peer
        return block_client.set_peer(new_peers);
    }

    // block rw
    if (FLAGS_rw_num != 0) {
        block_client.start(FLAGS_threads, FLAGS_write_percent, FLAGS_rw_num, FLAGS_local_path);
        block_client.join();
        return 0;
    }

    return 0;
}

