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

    $gk
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        audit              Audit of the message streams TODO
        brokers            Print online brokers from Zookeeper
        clusters           Register kafka clusters
        consumers          Print consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        lags               Display consumers lag for each topic each partition
        partition          Add partition num to a topic TODO
        peek               Peek kafka cluster messages ongoing
        producers          Display online producers TODO
        rebalance          Rebalance the load of brokers in a kafka cluster TODO
        top                Display top kafka cluster activities
        topics             Manage topics & partitions of a zone
        topology           Print server topology of kafka clusters
        underreplicated    Display under-replicated partitions
        zones              Print zones defined in /etc/gafka.cf
    
### TODO

- [X] bash autocomplete
- [X] display consumer lags
- [ ] add partitions on broker
- [ ] #partitions #leader on a broker
- [ ] rebalance a kafka cluster
- [ ] show offline partitions besides under-replicated partitions
- [ ] broker ownership management, what if a broker crash? how to find it
- [ ] zk need batch request
