############################################################
## Dockerfile to build kateway container images
#
# to run it:
# 0. curl -sSL https://get.docker.com/ | sh; service docker start
# 1. docker build -t "kateway:latest" .
# 2. docker run -d --name kateway -p 10191:9191 -p 10192:9192 -p 10193:9193 kateway:latest /go/bin/kateway -z prod -id 1 -log kateway.log -crashlog panic -influxdbaddr http://10.213.1.223:8086
#############################################################
FROM golang:1.5.3

MAINTAINER Funky Gao "funky.gao@gmail.com"

VOLUME /opt

WORKDIR /go/src/github.com/funkygao/gafka/
ADD .   /go/src/github.com/funkygao/gafka/

RUN ./build.sh -it kateway

RUN yum install -y git make wget ; \
    cd /opt ; \
    export PATH=$GOROOT/bin:$GOPATH/bin:$PATH ; \
    go get -d github.com/funkygao/gafka ; \
    cd $GOPATH/src/github.com/funkygao/gafka ; \
    ./build.sh -it kateway

ENV PATH $PATH:$GOPATH/bin

EXPOSE 9191 9192 9193
