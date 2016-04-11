# gk

Unified multi-datacenter multi-cluster kafka swiss-knife management console.

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
    

### TODO

- [ ] gk topbroker
