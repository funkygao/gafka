# gafka
Simplified multi-datacenter kafka clusters management console powered by golang.

### Install

    go get github.com/funkygao/gafka
    sudo cp $GOPATH/src/github.com/funkygao/gafka/etc/gafka.cf /etc

### Configuration

    /etc/gafka.cf

    {
        // a zone is groups of kafka cluster that share the same zookeeper ensemble.
        // zones may reside in different data centers.
        zones: [
            {
                name: "integration"
                zk: "10.77.144.87:10181,10.77.144.88:10181,10.77.144.89:10181"
            }
            {
                name: "test"
                zk: "10.77.144.101:10181,10.77.144.132:10181,10.77.144.182:10181"
            }
            {
                name: "production"
                zk: "10.209.33.69:2181,10.209.37.19:2181,10.209.37.68:2181"
            }
        ]
    
        loglevel: "debug"
        kafka_home: "/opt/kafka_2.10-0.8.1.1"
    }

### Usage

    $gafka
    
    usage: gafka [--version] [--help] <command> [<args>]
    
    Available commands are:
        brokers        Print online brokers from Zookeeper
        clusters       Register kafka clusters
        consumers      Print online consumers
        controllers    Print active controllers in kafka clusters
        lags           Display consumer lags TODO
        partition      Add partition num to a topic TODO
        peek           Peek kafka cluster messages ongoing
        rebalance      Rebalance the load of brokers in a kafka cluster TODO
        top            Display top kafka cluster activities TODO
        topics         Manage topics & partitions of a zone
        topology       Print server topology of kafka clusters
        zones          Print zones defined in /etc/gafka.cf

### TODO

- [X] bash autocomplete
- [ ] add/remove a broker
- [ ] rebalance a kafka cluster
- [ ] add partitions on broker
- [ ] show offline partitions besides under-replicated partitions
- [ ] display consumer lags
- [ ] if a broker crash, how do we know it's member of the cluster
- [ ] broker ownership management
- [ ] zk need batch request
