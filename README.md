# gafka 
    
                       __   _            
                      / _| | |           
       __ _    __ _  | |_  | | __   __ _ 
      / _` |  / _` | |  _| | |/ /  / _` |
     | (_| | | (_| | | |   |   <  | (_| |
      \__, |  \__,_| |_|   |_|\_\  \__,_|
       __/ |                             
      |___/                              

A full ecosystem for kafka/redis/PubSub/ElasticSearch/Zookeeper/haproxy.

Hope it can help you.

### Components

- [ehaproxy](https://github.com/funkygao/gafka/tree/master/cmd/ehaproxy)

  Elastic haproxy that sits in front of kateway.

- [kateway](https://github.com/funkygao/gafka/tree/master/cmd/kateway)

  A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message/job service.

- [actord](https://github.com/funkygao/gafka/tree/master/cmd/actord)

  kateway job scheduler and webhook dispatcher.

- [gk](https://github.com/funkygao/gafka/tree/master/cmd/gk)
 
  Unified multi-datacenter multi-cluster kafka swiss-knife management console.

- [zk](https://github.com/funkygao/gafka/tree/master/cmd/zk)

  A handy zookeeper CLI that supports recursive operation without any dependency.

- [es](https://github.com/funkygao/gafka/tree/master/cmd/es)

  ElasticSearch console.

- [kguard](https://github.com/funkygao/gafka/tree/master/cmd/kguard)

  Kafka clusters body guard that emits health info to InfluxDB and exports key warnings to zabbix for alerting.

### Install

    export PATH=$PATH:$GOPATH/bin

    #========================================
    # install go-bindata and go annotations
    #========================================
    go install github.com/jteeuwen/go-bindata/go-bindata
    go install github.com/funkygao/goannotation

    #========================================
    # install gafka
    #========================================
    go get github.com/funkygao/gafka
    cd $GOPATH/src/github.com/funkygao/gafka
    ./build.sh -a # build all components

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
- 10Billion messages per day
- peak load
  - 1Million message per second
  - 8TB transfered per hour
- 5TB redis

