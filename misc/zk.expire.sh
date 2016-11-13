#!/bin/sh
#=============================================
# a handy script to simulate zk session expire
#=============================================

while true; do
    kill -STOP $1
    sleep 20
    kill -CONT $1
    sleep 30
done
