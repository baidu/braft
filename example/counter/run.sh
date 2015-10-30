#!/bin/bash
#===============================================================================
#
#          FILE:  run.sh
# 
#         USAGE:  ./run.sh 
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

cd runtime/0
./counter_server -port=8500 -raft_start_port=8100 -raft_end_port=8102 -peers="127.0.0.1:8100:0,127.0.0.1:8101:0,127.0.0.1:8102:0" > std.log 2>&1 &
cd -

cd runtime/1
./counter_server -port=8501 -raft_start_port=8100 -raft_end_port=8102 -peers="127.0.0.1:8100:0,127.0.0.1:8101:0,127.0.0.1:8102:0" > std.log 2>&1 &
cd -

cd runtime/2
./counter_server -port=8502 -raft_start_port=8100 -raft_end_port=8102 -peers="127.0.0.1:8100:0,127.0.0.1:8101:0,127.0.0.1:8102:0" > std.log 2>&1 &
cd -
