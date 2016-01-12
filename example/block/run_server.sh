#!/bin/bash
#===============================================================================
#
#          FILE:  run_server.sh
# 
#         USAGE:  ./run_server.sh 
# 
#   DESCRIPTION:  
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  WangYao (), wangyao02@baidu.com
#       COMPANY:  Baidu.com, Inc
#       VERSION:  1.0
#       CREATED:  2015年10月30日 17时49分18秒 CST
#      REVISION:  ---
#===============================================================================

IP=`hostname -i`
#VALGRIND="/home/wangyao/opt/bin/valgrind --tool=memcheck --leak-check=full"
#HAS_VALGRIND="-has_valgrind"
#MAX_SEGMENT_SIZE="-raft_max_segment_size=524288"
CRASH_ON_FATAL="-crash_on_fatal_log=true"
#VLOG_LEVEL="-verbose=90"
BTHREAD_CONCURRENCY="-bthread_concurrency=50"
RPCZ="-enable_rpcz=true"
SYNC="-raft_sync=false"

cd runtime/0
${VALGRIND} ./block_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${RPCZ} ${MAX_SEGMENT_SIZE} ${SYNC} -ip_and_port="0.0.0.0:8200" -peers="${IP}:8200:0,${IP}:8201:0,${IP}:8202:0" > std.log 2>&1 &
cd -

cd runtime/1
${VALGRIND} ./block_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${RPCZ} ${MAX_SEGMENT_SIZE} ${SYNC} -ip_and_port="0.0.0.0:8201" -peers="${IP}:8200:0,${IP}:8201:0,${IP}:8202:0" > std.log 2>&1 &
cd -

cd runtime/2
${VALGRIND} ./block_server ${HAS_VALGRIND} ${BTHREAD_CONCURRENCY} ${CRASH_ON_FATAL} ${VLOG_LEVEL} ${RPCZ} ${MAX_SEGMENT_SIZE} ${SYNC} -ip_and_port="0.0.0.0:8202" -peers="${IP}:8200:0,${IP}:8201:0,${IP}:8202:0" > std.log 2>&1 &
cd -
