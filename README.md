# gafka 
A full ecosystem that is built around kafka powered by golang.

### Components

- gk
 
  Unified multi-datacenter multi-cluster kafka swiss-knife management console.

- zk

  A handy zookeeper CLI that supports recursive query.

- ehaproxy

  Elastic haproxy that sits in front of kateway.

- kateway

  A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message service.

- kguard

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

### Usage

    $gk
    Unified multi-datacenter multi-cluster kafka swiss-knife management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        ?                  FAQ
        brokers            Print online brokers from Zookeeper
        checkup            Health checkup of kafka runtime
        clusters           Register or display kafka clusters
        config             Display gk config file contents
        console            Interactive mode
        consumers          Print high level consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        deploy             Deploy a new kafka broker
        discover           Automatically discover online kafka clusters
        kateway            List/Config online kateway instances
        lags               Display high level consumers lag for each topic/partition
        lszk               List kafka related zookeepeer znode children
        migrate            Migrate given topic partition to specified broker ids
        mount              A FUSE module to mount a Kafka cluster in the filesystem
        offset             Manually reset consumer group offset
        partition          Add partition num to a topic for better parallel
        peek               Peek kafka cluster messages ongoing from any offset
        ping               Ping liveness of all registered brokers in a zone
        sample             Sample code of kafka producer/consumer in Java
        top                Unix “top” like utility for kafka
        topics             Manage kafka topics
        topology           Print server topology and balancing stats of kafka clusters
        underreplicated    Display under-replicated partitions
        zktop              Unix “top” like utility for ZooKeeper
        zones              Print zones defined in $HOME/.gafka.cf
        zookeeper          Monitor zone Zookeeper status by four letter word command
    
    $zk
    A CLI tool for Zookeeper
    
    usage: zk [--version] [--help] <command> [<args>]
    
    Available commands are:
        acl        Show znode ACL info
        console    Interactive mode
        create     Create znode with initial data
        dump       Dump permanent directories and contents of Zookeeper
        get        Show znode data
        ls         List znode children
        rm         Remove znode
        set        Write znode data
        stat       Show znode status info
        zones      Print zones defined in $HOME/.gafka.cf
   
### Status

Currently gafka ecosystem is widely used on production environment for daily jobs.

It manages:
- 4 data centers 
- 50+ kafka clusters
- 100+ kafka brokers
- 500+ kafka topics
- 2000+ kafka partitions
- 2B messages per day
- peak load
  - 0.6M message per second
  - 5G+ bps network bandwidth
