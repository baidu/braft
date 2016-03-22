#!/bin/bash
#
# Copyright (C) 2012 Roi Dayan <roid@mellanox.com>
#

TARGET=$1

usage() {
    echo "Usage: `basename $0` [rpm|deb]"
    exit 1
}

if [ "$TARGET" != "rpm" -a "$TARGET" != "deb" ]; then
    usage
fi

DIR=$(cd `dirname $0`; pwd)
BASE=`cd $DIR/.. ; pwd`
_TOP="$BASE/pkg"
SPEC="tgtd.spec"
LOG=/tmp/`basename $0`-$$.log

# get branch name
branch=`git branch | grep '^*' | sed 's/^..\(.*\)/\1/'`
# get version tag
version=`git describe --tags --abbrev=0 | sed "s/^v//g"`
# release is number of commits since the version tag
release=`git describe --tags | cut -d- -f2 | tr - _`

if [ "$version" = "$release" ]; then
    # no commits and release can't be empty
    release=0
fi

if [ "$branch" != "master" ]; then
    # if not under master branch include hash tag
    hash=`git rev-parse HEAD | cut -c 1-6`
    release="$release.$hash"
fi

echo "Building version: $version-$release"


cp_src() {
    local dest=$1
    cp -a conf $dest
    cp -a doc $dest
    cp -a scripts $dest
    cp -a usr $dest
    cp -a README $dest
    cp -a Makefile $dest
}

check() {
    local rc=$?
    local msg="$1"
    if (( rc )) ; then
        echo $msg
        exit 1
    fi
}

build_rpm() {
    name=scsi-target-utils-$version-$release
    TARBALL=$name.tgz
    SRPM=$_TOP/SRPMS/$name.src.rpm

    echo "Creating rpm build dirs under $_TOP"
    mkdir -p $_TOP/{RPMS,SRPMS,SOURCES,BUILD,SPECS,tmp}
    mkdir -p $_TOP/tmp/$name

    cp_src $_TOP/tmp/$name

    echo "Creating tgz $TARBALL"
    tar -czf $_TOP/SOURCES/$TARBALL -C $_TOP/tmp $name

    echo "Creating rpm"
    sed -r "s/^Version:(\s*).*/Version:\1$version/;s/^Release:(\s*).*/Release:\1$release/" scripts/$SPEC > $_TOP/SPECS/$SPEC
    rpmbuild -bs --define="_topdir $_TOP" $_TOP/SPECS/$SPEC
    check "Failed to create source rpm."

    rpmbuild -bb --define="_topdir $_TOP" $_TOP/SPECS/$SPEC > $LOG 2>&1
    check "Failed to build rpm. LOG: $LOG"
    # display created rpm files
    grep ^Wrote $LOG

    rm -fr $LOG
}

build_deb() {
    if ! which debuild >/dev/null 2>&1 ; then
        echo "Missing debuild. Please install devscripts package."
        exit 1
    fi
    name=tgt_$version
    TARBALL=$name.orig.tar.gz

    echo "Building under $_TOP/$name"
    mkdir -p $_TOP/$name
    cp_src $_TOP/$name
    tar -czf $_TOP/$TARBALL -C $_TOP $name

    mkdir -p $_TOP/$name/debian
    cp -a scripts/deb/* $_TOP/$name/debian
    cd $_TOP/$name
    sed -i -r "s/^tgt \(([0-9.-]+)\) (.*)/tgt \($version-$release\) \2/" debian/changelog
    debuild -uc -us
    check "Failed building deb package."
    cd ../..
    ls -l $_TOP/$name*.deb
}

cd $BASE
build_$TARGET
echo "Done."
