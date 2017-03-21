#!/bin/bash
# libraft - Quorum-based replication of states across machines.
# Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved 
# Author: The libraft authors

IP=`hostname -i`

#stats
#./counter_client -stats="${IP}:8100"
#./counter_client -stats="${IP}:8101"
#./counter_client -stats="${IP}:8102"

#put
./db_client -cluster_ns="list://${IP}:8100,${IP}:8101,${IP}:8102" -db_op="put" -key="test" -put_num=10
#get
#./db_client -cluster_ns="list://${IP}:8100,${IP}:8101,${IP}:8102" -db_op="get" -key="test_0"

#shutdown
#./db_client -shutdown="${IP}:8102"

#snapshot
#./db_client -snapshot="${IP}:8101"

#remove_peer
#./db_client -peers="${IP}:8100,${IP}:8101,${IP}:8102" -new_peers="${IP}:8100,${IP}:8101"

#add_peer
#./db_client -peers="${IP}:8100,${IP}:8101" -new_peers="${IP}:8100,${IP}:8101,${IP}:8102"

