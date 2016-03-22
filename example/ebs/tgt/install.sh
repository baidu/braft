#!/bin/bash

RETVAL=0
uid=`id | cut -d\( -f1 | cut -d= -f2`

# Only root can start the service
[ $uid -ne 0 ] && exit 4

cp -rp output/sbin/* /usr/sbin/
