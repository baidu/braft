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

IP=`hostname -i`

#stats
#./block_client -stats="${IP}:8200"
#./block_client -stats="${IP}:8201"
#./block_client -stats="${IP}:8202"

#write/read
# rm -rf 1G.data
./block_client -crash_on_fatal_log=true -peers="${IP}:8200,${IP}:8201,${IP}:8202" -rw_num=-1 -threads=5 -write_percent=80 -local_path=./1G.data
#./block_client -crash_on_fatal_log=true -peers="${IP}:8200,${IP}:8201,${IP}:8202" -rw_num=-1 -threads=10 -write_percent=80 -local_path=./1G.data > run.log 2>&1 &
#./block_client -peers="${IP}:8200,${IP}:8201,${IP}:8202" -rw_num=-1 -threads=10 -write_percent=80 -local_path=./1G.data

#shutdown
#./block_client -shutdown="${IP}:8202"

#snapshot
#./block_client -snapshot="${IP}:8201"

#remove_peer
#./block_client -peers="${IP}:8200,${IP}:8201,${IP}:8202" -new_peers="${IP}:8200,${IP}:8201"

#add_peer
#./block_client -peers="${IP}:8200,${IP}:8201" -new_peers="${IP}:8200,${IP}:8201,${IP}:8202"

