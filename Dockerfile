############################################################
## Dockerfile to build kateway container images
#
# to run it:
# 0. curl -sSL https://get.docker.com/ | sh; service docker start
# 1. docker build -t "kateway:latest" .
# 2. docker run -d --name kateway -p 10191:9191 -p 10192:9192 -p 10193:9193 kateway:latest /go/bin/kateway -z prod -id 1 -log kateway.log -crashlog panic -influxdbaddr http://10.213.1.223:8086
#############################################################
FROM golang:1.5.2

MAINTAINER Funky Gao "funky.gao@gmail.com"

ENV PATH $PATH:$GOPATH/bin

VOLUME /opt

WORKDIR /go/src/github.com/funkygao/gafka/
ADD .   /go/src/github.com/funkygao/gafka/

RUN go get ./cmd/kateway/...
RUN ./build.sh -it kateway

EXPOSE 9191 9192 9193
