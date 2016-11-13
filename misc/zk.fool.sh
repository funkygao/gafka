#!/bin/sh

while true; do
    kill -STOP $1
    sleep 20
    kill -CONT $1
    sleep 60
done
