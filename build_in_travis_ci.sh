if [ -z "$PURPOSE" ]; then
    echo "PURPOSE must be set"
    exit 1
fi
if [ -z "$CXX" ]; then
    echo "CXX must be set"
    exit 1
fi
if [ -z "$CC" ]; then
    echo "CC must be set"
    exit 1
fi

runcmd(){
    eval $@
    [[ $? != 0 ]] && {
        exit 1
    }
    return 0
}

echo "build combination: PURPOSE=$PURPOSE CXX=$CXX CC=$CC"

rm -rf bld && mkdir bld && cd bld
if [ "$PURPOSE" = "compile" ]; then
    if ! cmake ..; then
        echo "Fail to generate Makefile by cmake"
        exit 1
    fi
    make -j4
elif [ "$PURPOSE" = "unittest" ]; then
    if ! cmake -DBUILD_UNIT_TESTS=ON ..; then
        echo "Fail to generate Makefile by cmake"
        exit 1
    fi
    make -j4 && cd test && sh ../../test/run_tests.sh && cd ../
fi
