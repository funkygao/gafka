#!/bin/bash

if ! [ -x kateway ]; then
    echo "gk upgrade -k"
    exit 1
fi

killall kateway

grep -m 1 " bye\!" <(tail -f kateway.log)
echo "kateway gracefully shutdown"

mv -f kateway sbin/
nohup ./sbin/kateway -zone prod -id 1 -level trace -debughttp ":10194" -log kateway.log -crashlog panic

