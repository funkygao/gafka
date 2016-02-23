#!/bin/sh
curl -sSL https://get.docker.com/ | sh
yum upgrade device-mapper

service docker start
