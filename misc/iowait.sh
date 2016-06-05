#!/bin/sh
# 
#===========================================
# Find the processes that cause high IO wait
#
# yum -y install blktrace
# mount -t debugfs none /sys/kernel/debug
# blktrace -d /dev/sdm1 -o - | blkparse -i -
#===========================================
iostat -x 2 5
for x in `seq 1 1 10`; do ps -eo state,pid,cmd | grep "^D"; echo "----"; sleep 5; done

echo "cat /proc/#pid/io"
