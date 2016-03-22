#!/bin/bash
source mylocks.sh

# ls -l /dev/disk/by-path/ | grep 4065317065434198205 | cut -d'/' -f3
#
# dev_name = get_dev_name(vol_id:x:y) 
function get_dev_name() {
    local vol_str="$1"
    local dev_name=$(ls -l /dev/disk/by-path/ | grep $vol_str | cut -d'/' -f3)
    local retry=120

    for((i=0;i<$retry;i++))
    do 
        dev_name=$(ls -l /dev/disk/by-path/ | grep $vol_str | cut -d'/' -f3)
        if [ "$dev_name" != "" ]
        then
            break
        fi
        sleep 1
    done

    echo $dev_name
}


# get next tid
#
# next_tid = get_next_tid(void) 
function get_next_tid() {
    local max_tid=$(tgt-admin -s | grep Target | cut -d":" -f 1 | cut -d " " -f2| sort -g | tail -n 1)
    #next_tid=$(($max_tid+1))
    #let next_tid="${max_tid}+1"
    ((next_tid=max_tid+1))
    echo $next_tid
}

# example: 
#     $attach_volume.sh 3990098793024980126:x:y 
#      sdj
# dev_name = attach_volume(vol_id:x:y)

function attach_volume() {
	local my_ip=$(hostname -i)

	local TID=0
	local lockfile="/proc/partitions"
	# get lock
	mylock $lockfile
 
#	local target_num=`tgt-admin -s | grep 'Target' | wc -l`
#	target_num=`tgt-admin -s | grep 'Target' | awk END'{print $2}' | awk -F: '{print $1}'`
#	let TID="${target_num}+1"
    local TID=$(get_next_tid)

	local VOLUME_ID=$1

	tgtadm --lld iscsi --op new --mode target --tid ${TID} -T ebs.${VOLUME_ID}.${TID} || exit 1
	tgtadm --lld iscsi --op new --mode logicalunit --tid ${TID} --lun 1 --bstype bec_ebs --backing-store $VOLUME_ID || exit 1 
	tgtadm --lld iscsi --op bind --mode target --tid ${TID} -I ALL || exit 1 

	iscsiadm --mode discoverydb --type sendtargets --portal ${my_ip} --discover  > /dev/null 2>&1 || exit 1
	iscsiadm --mode node --targetname ebs.${VOLUME_ID}.${TID} --portal ${my_ip}:3260 --login  > /dev/null 2>&1  || exit 1
#	dev_name=$(iscsiadm -m session -P 3 | tail -n 1 | awk '{print $4}')
#    sleep 1
    dev_name=$(get_dev_name $VOLUME_ID)
	#release lock
	myunlock $lockfile

	echo $dev_name
}

# get_next_tid
attach_volume "$1"
