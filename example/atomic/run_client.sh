IP=`hostname -i`

./atomic_client -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -threads=300

