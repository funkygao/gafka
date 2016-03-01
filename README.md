# gafka 
Unified multi-datacenter multi-cluster kafka swiss-knife management console powered by golang.

### Features

- support multiple data centers of many kafka clusters
- a top alike tool provided showing real time producers activity
- kafka topic/partition routine OPS on a central console
- display global kafka brokers topology and check kafka balancing status
- peek ongoing messages at data center level
- explicit kafka clusters management
- monitor consumer lags
- automatically discovery of kafka clusters
- list under-replicated/offline brokers
- zookeeper management console included
- bash-autocomplete, alias, history supported

### Ecosystem

- gk
 
  Unified multi-datacenter multi-cluster kafka swiss-knife management console.

- zk

  A handy zookeeper CLI that supports recursive query.

- ehaproxy

  Elastic haproxy that sits in front of kateway

- kateway

  A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message service.

- kguard

  Kafka clusters body guard that emits health info to InfluxDB.

### Install

    go get github.com/funkygao/gafka

### Usage

    $gk
    Unified multi-datacenter multi-cluster kafka swiss-knife management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        ?                  Manual of gk
        alias              List all active aliasess
        brokers            Print online brokers from Zookeeper
        checkup            Health checkup of kafka runtime
        clusters           Register or display kafka clusters
        console            Interactive mode
        consumers          Print high level consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        deploy             Deploy a new kafka broker
        discover           Automatically discover online kafka clusters
        kateway            List online kateway instances
        lags               Display high level consumers lag for each topic/partition
        lszk               List kafka related zookeepeer znode children
        migrate            Migrate given topic partition to specified broker ids
        mount              A FUSE module to mount a Kafka cluster in the filesystem
        offset             Manually reset consumer group offset
        partition          Add partition num to a topic for better parallel
        peek               Peek kafka cluster messages ongoing from any offset
        ping               Ping liveness of all registered brokers in a zone
        top                Unix “top” like utility for kafka
        topics             Manage kafka topics
        topology           Print server topology and balancing stats of kafka clusters
        underreplicated    Display under-replicated partitions
        zktop              Unix “top” like utility for ZooKeeper
        zones              Print zones defined in $HOME/.gafka.cf
        zookeeper          Display zone Zookeeper status by four letter word command

### TODO

- [ ] display kafka server versions

