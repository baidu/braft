#!/bin/bash
#===============================================================================
#
#          FILE:  start.sh
# 
#         USAGE:  ./start.sh 
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
#       CREATED:  2015年10月30日 17时42分34秒 CST
#      REVISION:  ---
#===============================================================================

rm -rf runtime
mkdir -p runtime/0
cp counter_server runtime/0
mkdir -p runtime/1
cp counter_server runtime/1
mkdir -p runtime/2
cp counter_server runtime/2

 #./counter_server -port=8501 -raft_start_port=8100 -raft_end_port=8102 -peers="10.46.46.54:8100:0,10.46.46.54:8101:0,10.46.46.54:8102:0"
