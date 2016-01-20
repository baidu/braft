#!/bin/sh

# libraft - Quorum-based replication of states accross machines.
# Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

# Author: Zhangyi Chen (chenzhangyi01@baidu.com)
# Date: 2016/01/19 11:33:23

# Run test case
# sh run_sys_test.sh # which will run all the test cases
# sh run_sys_test.sh --case_name `wildcards' # run the test
#                                            # cases matches 
#                                            # the wildcard

cur_dir=$PWD
source $cur_dir/shflags

DEFINE_boolean 'rebuild' false 'Rebuild libraft'
DEFINE_string 'case_name' '*' 'Specific running case'
DEFINE_string 'client_name' 'sys_atomic_client' 'Binary name of client'
DEFINE_string 'server_name' 'sys_atomic_server' 'Binary name of server'

# Must include test_cases before parsing arguments owtherwise there will be
# `unknown flags' issues
source $cur_dir/test_cases.sh

FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

if ! [ -f Makefile ]; then
    comake2 -P || exit 1
fi

if [ $FLAGS_rebuild -eq $FLAGS_TRUE ]; then
    cd .. && make clean && make -sj4 && cd - && make clean && make -sj4
else
    make -sj
fi

if [ $? != 0 ]; then
    echo -e "\033[31;1mFail to make project\033[0m" >&2
    exit 1
fi

echo -e "\033[32;1mSuccessfully make project\033[0m"

# List all the function of the script
func_set=$(declare -F | awk '{print $3}')

# To record result
declare -a failed_tests
nfailed=0
declare -a successful_tests
nsucc=0

# Filtering function by the name
for test_func in $func_set; do
    # TODO(chenzhangyi01): $FLAGS_case_name can be multiple wildcards
    if [[ $test_func == test_* && $test_func == $FLAGS_case_name ]] ; then
        kill_all
        echo -e "\033[32;1mRunning $test_func\033[0m"
        rm -rf $test_func
        mkdir -p $test_func
        cd $test_func
        link ../$FLAGS_server_name $FLAGS_server_name \
            && link ../$FLAGS_client_name $FLAGS_client_name
        [ $? -eq 0 ] || exit 1
        $test_func
        test_result=$?
        find_fatal_logs_and_core_file
        if [ $test_result -eq 0 ]; then
            test_result=$?
        fi
        if [ $test_result -ne 0 ]; then
            echo -e "\033[31;1m$test_func failed\033[0m"
            failed_tests[$nfailed]="$test_func"
            let nfailed=($nfailed + 1)
        else
            echo -e "\033[32;1m$test_func succeeded\033[0m"
            successful_tests[$nsucc]="$test_func"
            let nsucc=($nsucc + 1)
        fi
        cd $cur_dir
    fi
done

exit_code=0

if [ $nsucc -gt 0 ]; then
    echo -e "\033[32;1mThe following ${nsucc} cases succeeded:\033[0m"
    for test_func in ${successful_tests[@]}; do
        echo -e "\033[32;1m$test_func\033[0m"
    done
fi

if [ $nfailed -gt 0 ]; then
    echo -e "\033[31;1mThe following ${nfailed} cases failed:\033[0m"
    for test_func in ${failed_tests[@]}; do
        echo -e "\033[31;1m$test_func\033[0m"
    done
    exit_code=1
fi

exit $exit_code
