#!/bin/bash

export LANG=en_US
REVISION=`svn info | grep Revision | awk '{print $2}'`
BUILDHOST=`hostname`
SVNURL=`svn info | grep URL | awk -Fsvn.baidu.com '{print $2}'`

VERSION="`cat version`-$REVISION BuildHost: $BUILDHOST $SVNURL"
echo $VERSION
