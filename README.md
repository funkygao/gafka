# gafka 

A full ecosystem that is built around kafka powered by golang.

### Components

- [ehaproxy](https://github.com/funkygao/gafka/tree/master/cmd/ehaproxy)

  Elastic haproxy that sits in front of kateway.

- [kateway](https://github.com/funkygao/gafka/tree/master/cmd/kateway)

  A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message/job service.

- [gk](https://github.com/funkygao/gafka/tree/master/cmd/gk)
 
  Unified multi-datacenter multi-cluster kafka swiss-knife management console.

- [zk](https://github.com/funkygao/gafka/tree/master/cmd/zk)

  A handy zookeeper CLI that supports recursive query.

- [kguard](https://github.com/funkygao/gafka/tree/master/cmd/kguard)

  Kafka clusters body guard that emits health info to InfluxDB.

- [gitlabtool](https://github.com/funkygao/gafka/tree/master/cmd/gitlabtool)

  A Pub/Sub sample application that watch all events on gitlab and evaluate KPI of programmers.

### Install

    export PATH=$PATH:$GOPATH/bin

    #========================================
    # install go-bindata first
    #========================================
    go get github.com/jteeuwen/go-bindata
    cd $GOPATH/src/github.com/jteeuwen/go-bindata/go-bindata
    go install

    #========================================
    # install gafka
    #========================================
    go get github.com/funkygao/gafka
    cd $GOPATH/src/github.com/funkygao/gafka
    go get ./...
    ./build.sh -h

    #========================================
    # install gafka command 'gk'
    #========================================
    ./build -i -t gk

    #========================================
    # try the gafka command 'gk'
    #========================================
    gk -h

### Status

Currently gafka manages:

- 4 data centers 
- 50+ kafka clusters
- 100+ kafka brokers
- 500+ kafka topics
- 2000+ kafka partitions
- 2Billion messages per day
- peak load
  - 0.6Million message per second
  - 5G+ bps network bandwidth
