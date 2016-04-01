# gafka 

A full ecosystem that is built around kafka powered by golang.

### Components

- [gk](https://github.com/funkygao/gafka/tree/master/cmd/gk)
 
  Unified multi-datacenter multi-cluster kafka swiss-knife management console.

- [zk](https://github.com/funkygao/gafka/tree/master/cmd/zk)

  A handy zookeeper CLI that supports recursive query.

- [ehaproxy](https://github.com/funkygao/gafka/tree/master/cmd/ehaproxy)

  Elastic haproxy that sits in front of kateway.

- [kateway](https://github.com/funkygao/gafka/tree/master/cmd/kateway)

  A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message/job service.

- [kguard](https://github.com/funkygao/gafka/tree/master/cmd/kguard)

  Kafka clusters body guard that emits health info to InfluxDB.

### Install

    export PATH=$PATH:$GOPATH/bin

    #========================================
    # install go-bindata first
    #========================================
    go get github.com/jteeuwen/go-bindata
    cd $GOPATH/src/github.com/jteeuwen/go-bindata/go-bindata
    go install

    #========================================
    # instal gafka command 'gk'
    #========================================
    go get github.com/funkygao/gafka
    cd $GOPATH/src/github.com/funkygao/gafka
    go get ./...
    ./build -i -t gk

    #========================================
    # try the gafka command 'gk'
    #========================================
    gk -h

### Status

Currently gafka ecosystem is widely used on production environment for daily jobs.

It manages:
- 4 data centers 
- 50+ kafka clusters
- 100+ kafka brokers
- 500+ kafka topics
- 2000+ kafka partitions
- 2Billion messages per day
- peak load
  - 0.6Million message per second
  - 5G+ bps network bandwidth
