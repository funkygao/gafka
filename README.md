# gafka
Unified multi-datacenter multi-kafka-clusters central management console powered by golang.

### Features

- support multiple data centers of many kafka clusters
- a top alike tool provided showing real time producers activity
- kafka topic/partition routine OPS on a central console
- display global kafka brokers topology and check kafka balancing status
- peek ongoing messages at data center level
- explicit kafka clusters management
- bash-autocomplete supported
- monitor consumer lags
- automatically discovery of kafka clusters
- list under-replicated/offline brokers

### Install

    go get github.com/funkygao/gafka

### Usage

    $gk
    
    Simplified multi-datacenter multi-kafka-clusters management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        audit              Audit of the message streams TODO
        brokers            Print online brokers from Zookeeper
        clusters           Register kafka clusters to a zone
        console            Interactive mode
        consumers          Print consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        discover           Automatically discover online kafka clusters
        lags               Display consumers lag for each topic each partition
        offlines           Display all offline brokers
        partition          Add partition num to a topic
        peek               Peek kafka cluster messages ongoing
        producers          Display online producers TODO
        rebalance          Rebalance the load of brokers in a kafka cluster TODO
        top                Display top kafka cluster activities
        topics             Manage topics & partitions of a zone
        topology           Print server topology and balancing stats of kafka clusters
        underreplicated    Display under-replicated partitions
        verifyreplicas     Validate that all replicas for a set of topics have the same data TODO
        zones              Print zones defined in $HOME/.gafka.cf
        zookeeper          Display zone Zookeeper status

### Configuration

    $HOME/gafka.cf

    {
        // a zone is groups of kafka cluster that share the same zookeeper ensemble.
        // zones may reside in different data centers.
        zones: [
            {
                name: "dev"
                zk: "10.77.144.101:10181,10.77.144.132:10181,10.77.144.182:10181"
            }
            {
                name: "test"
                zk: "10.77.144.101:10181,10.77.144.132:10181,10.77.144.182:10181"
            }
            {
                name: "staging"
                zk: "10.77.144.87:10181,10.77.144.88:10181,10.77.144.89:10181"
            }
            {
                name: "production"
                zk: "10.209.33.69:2181,10.209.37.19:2181,10.209.37.68:2181"
            }
        ]
    
        loglevel: "debug"
        kafka_home: "/opt/kafka_2.10-0.8.1.1"
    }

### TODO

- [ ] github.com/wvanbergen/kazoo-go bug
- [X] bash autocomplete
- [X] display consumer lags
- [X] add partitions on broker
- [X] #partitions #leader on a broker
- [X] topics command shows replicas count 
- [X] show offline partitions: leader broker id = -1 
- [X] auto discover cluster
- [X] total messages of a broker
- [X] prioritize kafka clusters
- [X] broker ownership management, what if a broker crash? how to find it
- [X] visualize the p(host)->c(hosts) relationship
- [ ] add tag for InfluxDB metrics
- [ ] health check of kafka brokers
- [ ] kdeploy
- [ ] rebalance a kafka cluster
- [ ] zk need batch request

### Monitor

- [ ] active controller switch
