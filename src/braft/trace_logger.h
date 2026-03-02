// Copyright (c) 2024 Specula Project Authors. All rights reserved.
// Trace instrumentation for TLA+ trace validation.
// Guarded by BRAFT_ENABLE_TRACE; zero-cost when disabled.

#ifndef BRAFT_TRACE_LOGGER_H
#define BRAFT_TRACE_LOGGER_H

#include <string>
#include <map>
#include <mutex>
#include <cstdio>
#include <cstdint>

#include "braft/configuration.h"
#include "braft/raft.h"

namespace braft {

class NodeImpl;
class BallotBox;
class LogManager;

// --------------------------------------------------------------------------
// Server ID Mapping
// --------------------------------------------------------------------------
// Maps braft PeerId (ip:port:idx) to stable short IDs ("s1", "s2", "s3")
// for human-readable traces and TLA+ alignment.

class TraceServerMap {
public:
    static TraceServerMap& instance();

    // Register a peer and return its short ID. Thread-safe.
    // Repeated calls with the same peer return the same ID.
    std::string register_peer(const PeerId& peer);

    // Look up a peer's short ID. Returns "" if not registered.
    std::string lookup(const PeerId& peer) const;

    // Reset all mappings (for testing).
    void reset();

private:
    TraceServerMap() : _next_id(1) {}
    mutable std::mutex _mu;
    std::map<std::string, std::string> _map;  // peer.to_string() -> "sN"
    int _next_id;
};

// --------------------------------------------------------------------------
// Trace State Snapshot
// --------------------------------------------------------------------------
// Captures the 6 core state fields from a node at one point in time.
// All callers must hold the appropriate locks before calling capture().

struct TraceState {
    int64_t term;
    const char* role;       // "Follower", "Candidate", "Leader"
    std::string votedFor;   // Short server ID or "" for Nil
    int64_t commitIndex;
    int64_t lastLogIndex;
    int64_t lastLogTerm;

    // Capture full state from a NodeImpl. Caller must hold _mutex.
    static TraceState capture(const NodeImpl* node);

    // Capture weak state: only term and role. Use from replicator bthreads
    // where full state is unavailable.
    static TraceState capture_weak(int64_t term, State role);
};

// --------------------------------------------------------------------------
// Trace Event Builder
// --------------------------------------------------------------------------
// Builds and emits a single NDJSON trace line.
//
// Usage:
//   TraceEvent("HandlePreVoteRequest")
//       .node(server_id)
//       .state(snap)
//       .msg_field("from", from_id)
//       .msg_field("to", to_id)
//       .msg_field("term", term)
//       .msg_field("granted", granted)
//       .emit();

class TraceEvent {
public:
    explicit TraceEvent(const char* name);

    // Set the node ID (short form, e.g. "s1").
    TraceEvent& node(const std::string& nid);

    // Attach state snapshot.
    TraceEvent& state(const TraceState& s);

    // Attach message fields (string value).
    TraceEvent& msg_field(const char* key, const std::string& val);

    // Attach message fields (integer value).
    TraceEvent& msg_field(const char* key, int64_t val);

    // Attach message fields (boolean value).
    TraceEvent& msg_field(const char* key, bool val);

    // Write the event to the trace log.
    void emit();

private:
    const char* _name;
    std::string _nid;
    TraceState _state;
    bool _has_state;

    // Simple KV pairs for msg fields (stored as JSON fragments).
    struct MsgField {
        const char* key;
        std::string json_val;  // Already JSON-encoded value
    };
    MsgField _msg_fields[8];
    int _msg_count;
};

// --------------------------------------------------------------------------
// Trace File Writer
// --------------------------------------------------------------------------
// Manages the output file for trace events. Thread-safe.

class TraceWriter {
public:
    static TraceWriter& instance();

    // Open trace file. Returns 0 on success.
    int open(const std::string& path);

    // Write a complete NDJSON line (including trailing newline).
    void write(const std::string& line);

    // Close the file.
    void close();

    bool is_open() const { return _fp != nullptr; }

private:
    TraceWriter() : _fp(nullptr) {}
    ~TraceWriter() { close(); }
    std::mutex _mu;
    FILE* _fp;
};

// --------------------------------------------------------------------------
// Convenience: map braft State enum to TLA+ role string
// --------------------------------------------------------------------------
inline const char* trace_role_str(State st) {
    switch (st) {
    case STATE_LEADER:       return "Leader";
    case STATE_TRANSFERRING: return "Leader";  // still leader in spec
    case STATE_CANDIDATE:    return "Candidate";
    case STATE_FOLLOWER:     return "Follower";
    default:                 return "Follower";
    }
}

// --------------------------------------------------------------------------
// Global enable check (gflag)
// --------------------------------------------------------------------------
bool trace_is_enabled();

// --------------------------------------------------------------------------
// Trace initialization (register peers, open file)
// --------------------------------------------------------------------------
// Call once per node during init. Registers self and all peers in the
// configuration, and opens the trace output file.
void trace_init(const PeerId& self, const Configuration& conf);

// --------------------------------------------------------------------------
// Macros for guarded instrumentation
// --------------------------------------------------------------------------
#ifdef BRAFT_ENABLE_TRACE

#define BRAFT_TRACE_IF_ENABLED(expr) \
    do { if (::braft::trace_is_enabled()) { expr; } } while (0)

#else

#define BRAFT_TRACE_IF_ENABLED(expr) do {} while (0)

#endif  // BRAFT_ENABLE_TRACE

}  // namespace braft

#endif  // BRAFT_TRACE_LOGGER_H
