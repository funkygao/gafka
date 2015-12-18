# gafka [![Build Status](https://travis-ci.org/funkygao/gafka.png?branch=master)](https://travis-ci.org/funkygao/gafka)
Unified multi-datacenter multi-kafka-clusters central management console powered by golang.

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
- bash-autocomplete supported

### Install

    go get github.com/funkygao/gafka

### Usage

    $gk
    
    Unified multi-datacenter multi-kafka-clusters management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        brokers            Print online brokers from Zookeeper
        checkup            Checkup of zookeepers and brokers
        clusters           Register kafka clusters to a zone
        consumers          Print consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        discover           Automatically discover online kafka clusters
        lags               Display consumers lag for each topic each partition
        lszk               List zookeepeer znode children
        partition          Add partition num to a topic
        peek               Peek kafka cluster messages ongoing
        ssh                ssh to a host through tunnel
        top                Unix “top” like utility for kafka
        topics             Manage topics & partitions of a zone
        topology           Print server topology and balancing stats of kafka clusters
        underreplicated    Display under-replicated partitions
        zktop              Unix “top” like utility for ZooKeeper
        zones              Print zones defined in $HOME/.gafka.cf
        zookeeper          Display zone Zookeeper status
    
### TODO

- [ ] gk top -who consumer
- [ ] github.com/wvanbergen/kazoo-go bug
- [ ] active controller switch history

