#!/bin/bash

IP=`hostname -i`
#VALGRIND="/home/wangyao/opt/bin/valgrind --tool=memcheck --leak-check=full"
#HAS_VALGRIND="-has_valgrind"
#MAX_SEGMENT_SIZE="-raft_max_segment_size=524288"
CRASH_ON_FATAL="-crash_on_fatal_log=true"
#VLOG_LEVEL="-verbose=3"
BTHREAD_CONCURRENCY="-bthread_concurrency=18 -stack_size_normal=10485760"
SYNC_EVERY_LOG="-raft_sync=true"

cd runtime/0
${VALGRIND} ./atomic_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${MAX_SEGMENT_SIZE} ${SYNC_EVERY_LOG} -ip_and_port="0.0.0.0:8300" -peers="${IP}:8300,${IP}:8301,${IP}:8302" > std.log 2>&1 &
cd -

cd runtime/1
${VALGRIND} ./atomic_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${MAX_SEGMENT_SIZE} ${SYNC_EVERY_LOG} -ip_and_port="0.0.0.0:8301" -peers="${IP}:8300,${IP}:8301,${IP}:8302" > std.log 2>&1 &
cd -

cd runtime/2
${VALGRIND} ./atomic_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${MAX_SEGMENT_SIZE} ${SYNC_EVERY_LOG} -ip_and_port="0.0.0.0:8302" -peers="${IP}:8300,${IP}:8301,${IP}:8302" > std.log 2>&1 &
cd -
