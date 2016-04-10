IP=`hostname -i`

./atomic_bench -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -threads=100

#./atomic_client -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -atomic_op="get" -atomic_id=0
#./atomic_client -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -atomic_op="set" -atomic_id=0 -atomic_val=0
#./atomic_client -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -atomic_op="cas" -atomic_id=0 -atomic_val=0 -atomic_new_val=1
#./atomic_client -cluster_ns="list://${IP}:8300,${IP}:8301,${IP}:8302" -atomic_op="remove_peer" -peer=${IP}:8300
#./atomic_client -cluster_ns="list://${IP}:8301,${IP}:8302" -atomic_op="add_peer" -peer=${IP}:8300

