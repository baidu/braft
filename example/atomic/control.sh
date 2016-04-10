#!/bin/sh

ulimit -n 10000
ulimit -c unlimited

nodes="10.75.27.19:8700 10.75.27.20:8700 10.75.27.25:8700 10.75.27.27:8700 10.75.27.28:8700"
self_ip=`ifconfig | grep "inet addr" | grep -v "127.0.0.1" | awk '{print substr($2,6,16)}'`
self_node=${self_ip}":8700"

peers=""
exclude_peers=""
# gen peers and exclude_peers
for node in $nodes
do
	#echo $node
	if [ -z $peers ];then
		peers=$node
	else
		peers=$peers","$node
	fi

	if [ $node == $self_node ];then
		continue
	fi
	if [ -z $exclude_peers ];then
		exclude_peers=$node
	else
		exclude_peers=$exclude_peers","$node
	fi
done

function help {
    echo "Usage: control.sh start|stop|restart|join|leave"
    exit 1
}

if [ $# -ne 1 ];then
    help
fi

case $1 in
    start)
        echo "start atomic_server "${self_node}
        rm -rf log data run.log core.*
        ./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -ip_and_port="0.0.0.0:8700" -peers=${peers} > run.log 2>&1 &
        ;;
    stop)
        echo "stop atomic_server "${self_node}
        killall -9 atomic_server
        ;;
    restart)
        echo "restart atomic_server "${self_node}
        ./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -ip_and_port="0.0.0.0:8700" -peers=${peers} > run.log 2>&1 &
        ;;
    join)
        echo "add atomic_server "${self_node}
        ./atomic_client -cluster_ns="list://"${exclude_peers} -atomic_op=add_peer -peer=${self_node}
        ;;
    leave)
        echo "remove atomic_server "${self_node}
        ./atomic_client -cluster_ns="list://"${peers} -atomic_op=remove_peer -peer=${self_node}
        ;;
    *)
        help
        ;;
esac

