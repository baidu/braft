# libraft - Quorum-based replication of states accross machines.
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

# Author: Zhangyi Chen (chenzhangyi01@baidu.com)
# Date: 2016/01/19 11:33:23

# WARNING: don't run this script directly.

# All test cases must be prefixed with `test_'

# For test the script only
function test_dummy() {
    echo "This is a dummy test"
}
#
#function test_failed_case() {
#    echo "This is a failed test"
#    return 1
#}

# TODO(chenzhangyi01), use auto port
DEFINE_integer 'start_port' 8600 'All the test servers start at the port' 

function test_normal() {
    cleanup_and_start_all_server
    start_client_and_wait "--num_requests=200000"
    rc=$?
    kill_all
    return $rc
}

function test_random_kill_server_and_restart_without_snapshot() {
    # don't do snapshot at all
    cleanup_and_start_all_server --snapshot_interval=999999999999
    random_kill_server_and_restart &
    killer_pid=$!
    start_client_and_wait "--num_requests=200000 --timeout_ms=1000"
    rc=$?
    kill -9 $killer_pid
    sleep 2
    kill_all
    return $rc
}

function test_random_kill_server_and_restart_with_snapshot() {
    # don't do snapshot at all
    cleanup_and_start_all_server --snapshot_interval=1
    random_kill_server_and_restart &
    killer_pid=$!
    start_client_and_wait "--num_requests=200000 --timeout_ms=1000"
    rc=$?
    kill -9 $killer_pid
    sleep 2
    kill_all
    return $rc
}

########### common utility

function find_fatal_logs_and_core_file() {
    rc=0
    core_files=$(find . -name core.*)
    if [ "$core_files" != "" ]; then
        echo -e "\033[31;1mFound core files: $core_files\033[0m"
        rc=1
    fi
    log_files=$(find . -type f | grep "\.log\|\.stderr\|\.stdout")
    for log in $log_files; do
        fatal_num=$(grep -c "FATAL\|^F" $log)
        if [ $fatal_num -ne 0 ]; then
            rc=1
            echo -e "\033[31;1mFound $fatal_num fatals in $log\033[0m"
        fi
    done
    return $rc
}

# We use global variables to pass arguments between different function
declare -a SERVER_PIDS

DEFAULT_NUM_SERVERS=${DEFAULT_NUM_SERVERS:-3}
DEFAULT_SNAPSHOT_INTERVAL=${DEFAULT_SNAPSHOT_INTERVAL:-30}
DEFAULT_RAFT_SYNC=${DEFAULT_RAFT_SYNC:-"true"}
DEFAULT_ELECTION_TIMEOUT_MS=${DEFAULT_ELECTION_TIMEOUT_MS:-500}

DEFINE_integer 'num_servers' $DEFAULT_NUM_SERVERS ' '
DEFINE_integer 'snapshot_interval' $DEFAULT_SNAPSHOT_INTERVAL ' '
DEFINE_string "raft_sync" "$DEFAULT_RAFT_SYNC" ' '
DEFINE_integer 'election_timeout_ms' $DEFAULT_ELECTION_TIMEOUT_MS ' '

PEER_LIST=""

function cleanup_and_start_all_server() {
    #reset intersted flags to default values before parsing arguments
    FLAGS_num_servers=$DEFAULT_NUM_SERVERS
    FLAGS_snapshot_interval=$DEFAULT_SNAPSHOT_INTERVAL
    FLAGS_raft_sync=$DEFAULT_RAFT_SYNC
    FLAGS_election_timeout_ms=$DEFAULT_ELECTION_TIMEOUT_MS
    # Parse arguments
    FLAGS "$@"
    if [ $FLAGS_num_servers -eq 0 ]; then
        echo -e "\033[31;1mYou must specific a positive number of servers\033[0m"
        return 1
    fi
    killall -9 $FLAGS_server_name 2>/dev/null
    rm server* -rf
    # reset global variables
    unset SERVER_PIDS
    IP=`hostname -i`
    PEER_LIST=""
    for (( i=0; i < $FLAGS_num_servers; ++i)); do
        server_dir="server$i"
        mkdir -p $server_dir || return 1
        let port=($FLAGS_start_port + $i)
        if [ "$PEER_LIST" != "" ]; then
            #Append comma to non-emtpy $PEER_LIST
            PEER_LIST+=','
        fi
        PEER_LIST+="$IP:$port"
    done
    for (( i =0 ; i < $FLAGS_num_servers; ++i)); do
        server_dir="server$i"
        cd $server_dir
        let port=($FLAGS_start_port + $i)
        server_pid=$(start_server $port)
        [ $? -eq 0 ] || return 1
        SERVER_PIDS[$i]=$server_pid
        echo "Started $server_dir, port=$port, pid=$server_pid"
        cd ..
    done
    return 0
}

# $1 is the port, ignoring all the other arguments
function start_server() {
    ip_and_port="0.0.0.0:$1"
    # start server
    ../$FLAGS_server_name  \
                --ip_and_port=$ip_and_port --peers="$PEER_LIST" \
                --raft_sync=$FLAGS_raft_sync \
                --election_timeout_ms=$FLAGS_election_timeout_ms \
                1> server.stdout 2> server.stderr &
    saved_rc=$?
    pid=$!
    # We can't use return as the return value in shell is a byte in [0, 255]
    echo $pid
    return $saved_rc
}

function kill_all() {
    killall -9 $FLAGS_server_name 2>/dev/null
    killall -9 $FLAGS_client_name 2>/dev/null
    # We don't care the return value of killall
    return 0
}

function start_client_and_wait() {
    cluster_ns="list://$PEER_LIST"
    ./$FLAGS_client_name --cluster_ns="$cluster_ns" $@ 2>&1 | tee client.stdout
}

function random_kill_server_and_restart() {
    let server_id_max=($FLAGS_num_servers -1)
    while [ true ]; do 
        for (( i=0 ; i < $FLAGS_num_servers; ++i)); do
            sleep 1
            server_dir="server$i"
            cd $server_dir
            server_pid=${SERVER_PIDS[$i]}
            kill -9 $server_pid
            sleep 1
            let port=($FLAGS_start_port + $i)
            server_pid=$(start_server $port)
            SERVER_PIDS[$i]=$server_pid
            cd ..
        done
    done
}

