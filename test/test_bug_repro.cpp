// Copyright (c) 2024 Baidu.com, Inc. All Rights Reserved
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

// Bug reproduction tests for braft.
// These tests demonstrate 3 bugs found via TLA+ model checking:
//   Bug A: Force-commit via config change (ballot_box.cpp commit_at pop loop)
//   Bug B: Leader lease PreVote asymmetry (become_leader resets follower_lease)
//   Bug C: elect_self persist failure doesn't roll back _current_term

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include "braft/node.h"
#include "braft/lease.h"
#include "braft/storage.h"
#include "../test/util.h"

namespace braft {
DECLARE_bool(raft_enable_leader_lease);
DECLARE_int32(raft_election_heartbeat_factor);
}

// ============================================================================
// Bug A & C: General test fixture
// ============================================================================
class BugReproTest : public testing::Test {
protected:
    void SetUp() override {
        ::system("rm -rf data");
        braft::FLAGS_raft_sync = false;
    }
    void TearDown() override {
        ::system("rm -rf data");
    }
};

// ============================================================================
// Bug B: Leader lease test fixture
// ============================================================================
class LeaderLeaseBugTest : public testing::Test {
protected:
    void SetUp() override {
        ::system("rm -rf data");
        braft::FLAGS_raft_sync = false;
        braft::FLAGS_raft_enable_leader_lease = true;
        braft::FLAGS_raft_election_heartbeat_factor = 3;
    }
    void TearDown() override {
        braft::FLAGS_raft_enable_leader_lease = false;
        ::system("rm -rf data");
    }
};

// ============================================================================
// Bug A: Force-Commit via Config Change
//
// Root cause: ballot_box.cpp commit_at() pops ALL entries from _pending_index
// to last_committed_index, even if intermediate entries' ballots were not
// individually granted. When a config change entry (with easier quorum) commits,
// earlier data entries get "swept along".
//
// Scenario: 4-node cluster {s1,s2,s3,s4}. Stop s3 and s4. Apply data entry E
// (needs 3/4 votes, only gets 2: s1+s2). Then remove_peer(s4) creates config
// entry C with ballot {s1,s2,s3}, quorum=2. s1+s2 grant C -> committed.
// The pop loop sweeps E along with C, force-committing E without quorum.
//
// Assumptions:
// - Operations complete before leader detects missing nodes (realistic on
//   localhost with election_timeout=3000ms)
// - Non-invasive: uses only Cluster API (start/stop/apply/remove_peer)
// ============================================================================
TEST_F(BugReproTest, ForceCommitViaConfigChange) {
    // 4-node cluster
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < 4; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // Use default election timeout (3000ms) so leader won't step down quickly
    Cluster cluster("unittest", peers, 3000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // Wait for leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // Apply initial entries so the cluster is in a stable state
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "initial: %d", i);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();
    cluster.ensure_same();
    LOG(WARNING) << "initial entries applied and replicated";

    // Record the leader's FSM applied count before the bug-triggering entry
    MockFSM* leader_fsm = static_cast<MockFSM*>(
        leader->_impl->_options.fsm);
    leader_fsm->lock();
    size_t logs_before = leader_fsm->logs.size();
    leader_fsm->unlock();
    LOG(WARNING) << "leader FSM log count before test entry: " << logs_before;

    // Identify followers: stop exactly 2 followers (NOT the leader).
    // We need the leader + 1 follower alive = 2 out of 4 (not a majority).
    std::vector<braft::Node*> followers;
    cluster.followers(&followers);
    ASSERT_EQ(3u, followers.size());

    // We'll stop followers[0] and followers[1], keep followers[2] alive
    butil::EndPoint stop_addr1 = followers[0]->node_id().peer_id.addr;
    butil::EndPoint stop_addr2 = followers[1]->node_id().peer_id.addr;
    braft::PeerId peer_to_remove = followers[1]->node_id().peer_id;
    LOG(WARNING) << "stopping follower1=" << stop_addr1
                 << " and follower2=" << stop_addr2;
    cluster.stop(stop_addr1);
    cluster.stop(stop_addr2);

    // Small sleep to let the cluster notice the stops
    bthread_usleep(100 * 1000);

    // Apply data entry E. With 4-node config, quorum = 3.
    // Only s1 (leader, self-grant) and s2 can ACK. That's 2 votes -- not enough.
    // E will remain pending (uncommitted).
    // We use a raw closure to track whether E gets committed.
    bthread::CountdownEvent apply_cond(1);
    {
        butil::IOBuf data;
        data.append("BUG_A_TEST_ENTRY");

        braft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&apply_cond, 0);
        leader->apply(task);
    }

    // Give some time for E to be replicated to s2 (but not committed)
    bthread_usleep(200 * 1000);

    // Now remove s4 from the configuration. This is a single-peer change
    // (nchanges=1), so it skips joint consensus. The config entry C gets
    // ballot {s1,s2,s3} with quorum = 2. s1 self-grants (quorum->1), s2 ACKs
    // (quorum->0) -> COMMITTED.
    //
    // The commit_at pop loop then sweeps from _pending_index (=E's index)
    // through last_committed_index (=C's index), force-committing E even
    // though E's ballot only had 2/4 votes (not 3/4 majority).
    bthread::CountdownEvent remove_cond(1);
    LOG(WARNING) << "removing peer " << peer_to_remove;
    leader->remove_peer(peer_to_remove, NEW_REMOVEPEERCLOSURE(&remove_cond, 0));
    remove_cond.wait();
    LOG(WARNING) << "remove_peer completed successfully";

    // The apply closure for E should also have fired (E was force-committed).
    // Since commit_at's pop loop sweeps E along with C in the same call,
    // E should already be committed by the time remove_peer's callback fires.
    // Give the FSM caller a moment to apply.
    bthread_usleep(500 * 1000);

    // === ASSERTIONS ===

    // 1. remove_peer succeeded (already asserted via NEW_REMOVEPEERCLOSURE)
    LOG(WARNING) << "Assertion 1: remove_peer callback succeeded";

    // 3. E was applied to leader's FSM
    leader_fsm->lock();
    size_t logs_after = leader_fsm->logs.size();
    bool found_test_entry = false;
    for (size_t i = logs_before; i < logs_after; i++) {
        if (leader_fsm->logs[i].to_string().find("BUG_A_TEST_ENTRY") !=
            std::string::npos) {
            found_test_entry = true;
            break;
        }
    }
    leader_fsm->unlock();

    ASSERT_TRUE(found_test_entry)
        << "BUG CONFIRMED: Entry E was force-committed to leader's FSM "
        << "despite only having 2 of 4 votes (needed 3). "
        << "The commit_at pop loop swept E along with the config change entry.";
    LOG(WARNING) << "BUG A CONFIRMED: Entry E was force-committed with only "
                 << "2/4 votes. logs_before=" << logs_before
                 << " logs_after=" << logs_after;

    // 4. E only had 2 of 4 votes (s1 self-grant + s2 ACK).
    // s3 and s4 were stopped and couldn't vote. This means E was committed
    // without a majority of the ORIGINAL 4-node configuration.
    // (This is implicit from the test setup -- s3 and s4 are stopped.)
    LOG(WARNING) << "Entry E had at most 2 votes (s1+s2) out of 4-node config "
                 << "(s3 and s4 were stopped). Quorum required: 3. "
                 << "This violates Raft's commit safety.";

    cluster.stop_all();
}

// ============================================================================
// Bug B: Leader Lease PreVote Asymmetry
//
// Root cause: become_leader() calls _follower_lease.reset(), which sets
// _last_leader_timestamp = 0. This makes votable_time_from_now() always
// return 0 on the leader, meaning the leader always grants PreVote requests
// regardless of leader lease.
//
// Code path:
//   node.cpp:2001 -> _follower_lease.reset()
//   lease.cpp:134-137 -> _last_leader_timestamp = 0
//   lease.cpp:111-123 -> with timestamp=0, now >> votable_timestamp -> returns 0
//   node.cpp:2219 -> granted = (votable_time == 0) -> always true for leader
//
// Assumptions:
// - Reads internal state via -Dprivate=public (standard braft test practice,
//   see test/CMakeLists.txt line 11)
// - Non-invasive: only reads state, no mutations
// ============================================================================
TEST_F(LeaderLeaseBugTest, LeaderLeasePreVoteAsymmetry) {
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // Use shorter timeouts for faster test execution
    Cluster cluster("unittest", peers, 500, 10);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // Wait for leader election
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader elected: " << leader->node_id();

    // Wait for lease to become VALID
    braft::LeaderLeaseStatus lease_status;
    int64_t start_ms = butil::monotonic_time_ms();
    do {
        bthread_usleep(100 * 1000);
        leader->get_leader_lease_status(&lease_status);
    } while (lease_status.state != braft::LEASE_VALID &&
             butil::monotonic_time_ms() - start_ms < 5000);
    ASSERT_EQ(lease_status.state, braft::LEASE_VALID)
        << "Leader lease should be VALID";
    LOG(WARNING) << "leader lease is VALID";

    // Extra sleep to ensure followers have received heartbeats and renewed
    // their follower leases
    bthread_usleep(600 * 1000);

    // === BUG CHECK: Leader's follower lease ===
    // On the leader, _follower_lease was reset() in become_leader(), setting
    // _last_leader_timestamp = 0. This makes votable_time_from_now() return 0
    // (because now >> _last_leader_timestamp + election_timeout + clock_drift).
    int64_t leader_votable_time =
        leader->_impl->_follower_lease.votable_time_from_now();
    int64_t leader_last_timestamp =
        leader->_impl->_follower_lease.last_leader_timestamp();

    LOG(WARNING) << "Leader follower_lease: votable_time_from_now="
                 << leader_votable_time
                 << " last_leader_timestamp=" << leader_last_timestamp;

    // ASSERTION 1: Leader's votable_time_from_now() == 0
    // BUG: The leader always returns 0, meaning it would always grant PreVote
    // requests. A correct implementation would either:
    //   a) Not use follower_lease on the leader at all, or
    //   b) Keep it updated so it reflects the leader's own lease validity
    ASSERT_EQ(0, leader_votable_time)
        << "BUG CONFIRMED: Leader's votable_time_from_now() is 0 because "
        << "become_leader() called _follower_lease.reset() which zeroed "
        << "_last_leader_timestamp. The leader will always grant PreVote.";

    // ASSERTION 2: Leader's _last_leader_timestamp == 0 (root cause)
    ASSERT_EQ(0, leader_last_timestamp)
        << "BUG ROOT CAUSE: _last_leader_timestamp is 0 on the leader "
        << "because become_leader() calls _follower_lease.reset()";

    // === CONTRAST: Followers' follower lease ===
    // Followers have recently received heartbeats, so their follower_lease
    // should have a recent _last_leader_timestamp and votable_time > 0.
    std::vector<braft::Node*> followers;
    cluster.followers(&followers);
    ASSERT_GE(followers.size(), 1u);

    bool any_follower_has_lease = false;
    for (size_t i = 0; i < followers.size(); i++) {
        int64_t follower_votable_time =
            followers[i]->_impl->_follower_lease.votable_time_from_now();
        int64_t follower_last_timestamp =
            followers[i]->_impl->_follower_lease.last_leader_timestamp();

        LOG(WARNING) << "Follower " << followers[i]->node_id()
                     << " follower_lease: votable_time_from_now="
                     << follower_votable_time
                     << " last_leader_timestamp=" << follower_last_timestamp;

        // ASSERTION 3: Followers' votable_time_from_now() > 0
        // Followers recently got heartbeats, so they should reject PreVote
        // (their lease hasn't expired yet).
        if (follower_votable_time > 0) {
            any_follower_has_lease = true;
        }

        // Follower's timestamp should be recent (non-zero)
        ASSERT_GT(follower_last_timestamp, 0)
            << "Follower should have a non-zero last_leader_timestamp "
            << "after receiving heartbeats";
    }

    ASSERT_TRUE(any_follower_has_lease)
        << "At least one follower should have votable_time > 0 "
        << "(rejecting PreVote during valid lease)";

    LOG(WARNING) << "BUG B CONFIRMED: Asymmetry demonstrated. "
                 << "Leader votable_time=0 (always grants PreVote) vs "
                 << "followers votable_time>0 (correctly reject PreVote). "
                 << "Root cause: become_leader() calls _follower_lease.reset() "
                 << "which zeros _last_leader_timestamp.";

    cluster.stop_all();
}

// ============================================================================
// Bug C: elect_self Persist Failure Doesn't Roll Back _current_term
//
// Root cause: In elect_self():
//   1. _current_term++ (line 1752) -- term incremented in memory
//   2. request_peers_to_vote(peers, ...) (line 1787) -- RPCs sent with new term
//   3. _meta_storage->set_term_and_votedfor(...) (line 1790) -- persist attempt
//   4. If persist fails: _voted_id.reset() but _current_term NOT rolled back
//
// Result: in-memory term > persisted term, violating term monotonicity on
// crash-restart.
//
// ASSUMPTIONS (disclosed):
// - MILDLY INVASIVE: Swaps _meta_storage pointer at runtime. This is the
//   minimum injection needed to trigger the persist failure path.
// - PERSIST FAILURE IS UNREALISTICALLY STRONG: raft_sync_meta defaults to
//   false, so the only realistic failure is ENOSPC. The mock simulates a
//   scenario that rarely occurs in production.
// - The bug's real-world impact: if a node does experience disk failure during
//   elect_self, it enters an inconsistent state where a crash-restart could
//   violate term monotonicity.
// ============================================================================

// A delegating RaftMetaStorage that can inject failures on set_term_and_votedfor
class FailableMetaStorage : public braft::RaftMetaStorage {
public:
    explicit FailableMetaStorage(braft::RaftMetaStorage* delegate)
        : _delegate(delegate), _fail_on_set(false) {}

    virtual ~FailableMetaStorage() {}

    void set_fail(bool fail) { _fail_on_set = fail; }

    virtual butil::Status init() override {
        return _delegate->init();
    }

    virtual butil::Status set_term_and_votedfor(
            const int64_t term, const braft::PeerId& peer_id,
            const braft::VersionedGroupId& group) override {
        if (_fail_on_set) {
            LOG(WARNING) << "FailableMetaStorage: INJECTING FAILURE on "
                         << "set_term_and_votedfor(term=" << term
                         << ", peer=" << peer_id << ")";
            butil::Status st;
            st.set_error(EIO, "Injected disk failure");
            return st;
        }
        return _delegate->set_term_and_votedfor(term, peer_id, group);
    }

    virtual butil::Status get_term_and_votedfor(
            int64_t* term, braft::PeerId* peer_id,
            const braft::VersionedGroupId& group) override {
        return _delegate->get_term_and_votedfor(term, peer_id, group);
    }

    virtual braft::RaftMetaStorage* new_instance(
            const std::string& uri) const override {
        return _delegate->new_instance(uri);
    }

    virtual butil::Status gc_instance(
            const std::string& uri,
            const braft::VersionedGroupId& vgid) const override {
        return _delegate->gc_instance(uri, vgid);
    }

    braft::RaftMetaStorage* delegate() const { return _delegate; }

private:
    braft::RaftMetaStorage* _delegate;
    bool _fail_on_set;
};

TEST_F(BugReproTest, ElectSelfPersistFailureNoTermRollback) {
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // Use shorter election timeout so elections happen faster after leader stop
    Cluster cluster("unittest", peers, 1000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader elected: " << leader->node_id();

    // Pick a follower to inject the failure into
    std::vector<braft::Node*> followers;
    cluster.followers(&followers);
    ASSERT_GE(followers.size(), 1u);
    braft::Node* target = followers[0];
    LOG(WARNING) << "target follower: " << target->node_id();

    // Record the follower's current term (persisted and in-memory should match)
    int64_t term_before = target->_impl->_current_term;
    LOG(WARNING) << "target in-memory term before: " << term_before;

    // Verify persisted term matches
    {
        int64_t persisted_term = 0;
        braft::PeerId persisted_voted;
        butil::Status st = target->_impl->_meta_storage->get_term_and_votedfor(
            &persisted_term, &persisted_voted, target->_impl->_v_group_id);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(term_before, persisted_term)
            << "In-memory and persisted term should match initially";
        LOG(WARNING) << "persisted term before: " << persisted_term;
    }

    // Swap the follower's _meta_storage with our failable wrapper
    braft::RaftMetaStorage* original_storage = target->_impl->_meta_storage;
    FailableMetaStorage* failable = new FailableMetaStorage(original_storage);

    // Enable failure mode BEFORE swapping, so we don't miss the window
    failable->set_fail(true);
    target->_impl->_meta_storage = failable;
    LOG(WARNING) << "injected FailableMetaStorage into target";

    // Stop the leader. This will cause followers' election timers to fire,
    // leading to pre_vote -> elect_self on the target follower.
    butil::EndPoint leader_addr = leader->node_id().peer_id.addr;
    LOG(WARNING) << "stopping leader " << leader_addr;
    cluster.stop(leader_addr);

    // Wait for the target to attempt election. With election_timeout=1000ms,
    // it should try within a few seconds. We poll the term.
    int64_t deadline_ms = butil::monotonic_time_ms() + 10000;
    while (butil::monotonic_time_ms() < deadline_ms) {
        int64_t current_term = target->_impl->_current_term;
        if (current_term > term_before) {
            LOG(WARNING) << "target in-memory term incremented to "
                         << current_term << " (was " << term_before << ")";
            break;
        }
        bthread_usleep(50 * 1000);
    }

    // After the election attempt with persist failure, check the state.
    // The in-memory term may have been incremented by elect_self() or by
    // step_down when receiving responses. Let's check the current state.
    int64_t term_after_memory = target->_impl->_current_term;

    // Check persisted term -- this goes through the failable wrapper to the
    // real storage, but get_term_and_votedfor is NOT failed.
    int64_t persisted_term_after = 0;
    braft::PeerId persisted_voted_after;
    // Use the original storage directly to get the actual persisted value
    butil::Status st = original_storage->get_term_and_votedfor(
        &persisted_term_after, &persisted_voted_after,
        target->_impl->_v_group_id);
    ASSERT_TRUE(st.ok());

    LOG(WARNING) << "After election attempt with persist failure:";
    LOG(WARNING) << "  in-memory _current_term = " << term_after_memory;
    LOG(WARNING) << "  persisted term          = " << persisted_term_after;
    LOG(WARNING) << "  term_before             = " << term_before;

    // === ASSERTIONS ===

    // The in-memory term should have been incremented (elect_self does
    // _current_term++ before the persist call). Even if the persist fails,
    // the in-memory term stays incremented.
    // Note: the term may have been incremented multiple times if the node
    // attempted election multiple times, or if it received vote responses
    // that caused step_down to a higher term. We just need term > term_before.
    ASSERT_GT(term_after_memory, term_before)
        << "In-memory term should have been incremented by elect_self()";

    // The persisted term should still be the old value (or at most equal to
    // what was persisted before), because the set_term_and_votedfor call failed.
    // BUG: _current_term was incremented in memory but NOT persisted.
    ASSERT_EQ(term_before, persisted_term_after)
        << "BUG CONFIRMED: Persisted term should still be " << term_before
        << " because set_term_and_votedfor failed, but in-memory term is "
        << term_after_memory << ". This means on crash-restart, the node "
        << "would restart with term=" << persisted_term_after
        << " while peers may have seen term=" << term_after_memory
        << ", violating term monotonicity.";

    LOG(WARNING) << "BUG C CONFIRMED: In-memory term (" << term_after_memory
                 << ") > persisted term (" << persisted_term_after
                 << "). elect_self() incremented _current_term but the "
                 << "persist call failed. _current_term was NOT rolled back. "
                 << "A crash-restart would violate term monotonicity.";

    // === CLEANUP ===
    // Restore original _meta_storage before cluster teardown
    failable->set_fail(false);
    target->_impl->_meta_storage = original_storage;
    LOG(WARNING) << "restored original _meta_storage";

    // Delete the failable wrapper (doesn't own the delegate)
    delete failable;

    cluster.stop_all();
}
