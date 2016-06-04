#!/bin/sh
# 
#===========================================
# Find the processes that cause high IO wait
#===========================================
iostat -x 2 5
for x in `seq 1 1 10`; do ps -eo state,pid,cmd | grep "^D"; echo "----"; sleep 5; done

