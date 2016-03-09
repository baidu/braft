IP=`hostname -i`

./block_client -cluster_ns="list://${IP}:8400,${IP}:8401,${IP}:8402" $@

