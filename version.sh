#!/bin/bash

export LC_ALL=en_US.utf8
REVISION=`svn info | grep Revision | awk '{print $2}'`
BUILDHOST=`hostname`
SVNURL=`svn info | grep URL | awk -Fsvn.baidu.com '{print $2}'`

VERSION="`cat version`-$REVISION BuildHost: $BUILDHOST $SVNURL"
echo $VERSION
