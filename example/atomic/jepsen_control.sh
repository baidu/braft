#!/bin/sh

ulimit -n 10000
ulimit -c unlimited

nodes="172.17.0.2:8700 172.17.0.3:8700 172.17.0.4:8700 172.17.0.5:8700 172.17.0.6:8700"
#self_ip=`ifconfig | grep "inet addr" | grep -v "127.0.0.1" | awk '{print substr($2,6,16)}'`
self_ip=`ifconfig | grep "mask" | grep -v "127.0.0.1" | awk '{print $2}'`
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
    echo "Usage: jepsen_control.sh boot|start|stop|restart|join|leave"
    exit 1
}

if [ $# -ne 1 ];then
    help
fi

case $1 in
    boot)
        echo "boot atomic_server "${self_node}
	killall -9 atomic_server
        rm -rf log data run.log core.* && mkdir log
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &
        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 > run.log 2>&1 &
	sleep 1
	./braft_cli reset_peer --group=Atomic --peer=${self_node} --new_peers=${peers}
        ;;
    start)
        echo "start atomic_server "${self_node}
        rm -rf log data run.log core.* && mkdir log
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &
        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 > run.log 2>&1 &
        ;;
    stop)
        echo "stop atomic_server "${self_node}
        killall -9 atomic_server
        ;;
    restart)
        echo "restart atomic_server "${self_node}
        killall -9 atomic_server
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &
        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 > run.log 2>&1 &
        ;;
    join)
        echo "add atomic_server "${self_node}
	./braft_cli add_peer --group=Atomic --peer=${self_node} --conf=${exclude_peers}
        ;;
    leave)
        echo "remove atomic_server "${self_node}
	./braft_cli remove_peer --group=Atomic --peer=${self_node} --conf=${peers}
        ;;
    *)
        help
        ;;
esac

