#!/bin/bash

export MAC=64

workdir=`pwd`
output=${workdir}/output
rm -rf ${output}

mkdir ${output}/tgtd -p

make clean
make all BEC_EBS=1

cp -rp ${output}/sbin ${output}/tgtd
cp -rp ${output}/lib ${output}/tgtd
cp -rp ${output}/etc ${output}/tgtd
cp -rp ${output}/conf ${output}/tgtd
cp -rp ${output}/bin ${output}/tgtd
cp -rp install.sh ${output}/tgtd/

exit $?
