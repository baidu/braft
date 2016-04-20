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
#./counter_client -stats="${IP}:8100"
#./counter_client -stats="${IP}:8101"
#./counter_client -stats="${IP}:8102"

#fetch_and_add
#./counter_client -peers="${IP}:8100,${IP}:8101,${IP}:8102" -fetch_and_add_num=1000 -threads=10
./counter_client -peers="${IP}:8100,${IP}:8101,${IP}:8102" -fetch_and_add_num=-1 -threads=300

#shutdown
#./counter_client -shutdown="${IP}:8102"

#snapshot
#./counter_client -snapshot="${IP}:8101"

#remove_peer
#./counter_client -peers="${IP}:8100,${IP}:8101,${IP}:8102" -new_peers="${IP}:8100,${IP}:8101"

#add_peer
#./counter_client -peers="${IP}:8100,${IP}:8101" -new_peers="${IP}:8100,${IP}:8101,${IP}:8102"

