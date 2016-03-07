#!/bin/bash
# libraft - Quorum-based replication of states across machines.
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved 
# Author: The libraft authors

# source shflags from current directory
mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/../../../baidu-rpc/tools/shflags

# define command-line flags
DEFINE_string crash_on_fatal 'true' 'Crash on fatal log'
DEFINE_integer bthread_concurrency '18' 'Number of worker pthreads'
DEFINE_string sync 'false' 'fsync each time'
DEFINE_string valgrind 'false' 'Run in valgrind'
DEFINE_integer max_segment_size '536870912' 'Max segment size'
DEFINE_integer server_num '3' 'Number of servers'
DEFINE_boolean clean 1 'Remove old "runtime" dir before running'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

# The alias for printing to stderr
alias error=">&2 echo block: "

IP=`hostname -i`
if [ "$FLAGS_valgrind" == "true" ] && [ $(which valgrind) ] ; then
    VALGRIND="valgrind --tool=memcheck --leak-check=full"
    HAS_VALGRIND="-has_valgrind"
fi

raft_peers=""
for ((i=0; i<$FLAGS_server_num; ++i)); do
    raft_peers="${raft_peers}${IP}:$((8200+i)):0,"
done

if [ "$FLAGS_clean" == "0" ]; then
    rm -rf runtime
fi

for ((i=0; i<$FLAGS_server_num; ++i)); do
    mkdir -p runtime/$i
    cp comlog.conf runtime/$i
    cp ./block_server runtime/$i
    cd runtime/$i
    ${VALGRIND} ./block_server ${HAS_VALGRIND} \
        -bthread_concurrency=${FLAGS_bthread_concurrency}\
        -crash_on_fatal_log=${FLAGS_crash_on_fatal} \
        -raft_max_segment_size=${FLAGS_max_segment_size} \
        -raft_sync=${FLAGS_sync} \
        -ip_and_port="0.0.0.0:$((8200+i))" -peers="${raft_peers}" > std.log 2>&1 &
    cd ../..
done
