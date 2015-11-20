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

cd runtime/0
${VALGRIND} ./counter_server ${HAS_VALGRIND} -bthread_concurrency=18 -verbose=90 -ip_and_port="0.0.0.0:8100" -peers="${IP}:8100:0,${IP}:8101:0,${IP}:8102:0" > std.log 2>&1 &
cd -

cd runtime/1
${VALGRIND} ./counter_server ${HAS_VALGRIND} -bthread_concurrency=18 -verbose=90 -ip_and_port="0.0.0.0:8101" -peers="${IP}:8100:0,${IP}:8101:0,${IP}:8102:0" > std.log 2>&1 &
cd -

cd runtime/2
${VALGRIND} ./counter_server ${HAS_VALGRIND} -bthread_concurrency=18 -verbose=90 -ip_and_port="0.0.0.0:8102" -peers="${IP}:8100:0,${IP}:8101:0,${IP}:8102:0" > std.log 2>&1 &
cd -
