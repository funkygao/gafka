# gk

Unified multi-datacenter multi-cluster kafka swiss-knife management console.

### Usage

    $gk
    Unified multi-datacenter multi-cluster kafka swiss-knife management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
        ?                  FAQ
        agent              Starts the gk agent and runs until an interrupt is received
        alias              Display all aliases defined in $HOME/.gafka.cf
        brokers            Print online brokers from Zookeeper
        checkup            Health checkup of kafka runtime
        clusters           Register or display kafka clusters
        config             Display gk config file contents
        console            Interactive mode
        consumers          Print high level consumer groups from Zookeeper
        controllers        Print active controllers in kafka clusters
        deploy             Deploy a new kafka broker on localhost
        disable            Disable Pub topic partition
        discover           Automatically discover online kafka clusters
        haproxy            Query haproxy cluster for load stats
        histogram          Histogram of kafka produced messages and network volumn
        kateway            List/Config online kateway instances
        kguard             List online kguard instances
        lags               Display online high level consumers lag on a topic
        leader             Restore the leadership balance for a given topic partition
        lszk               List kafka related zookeepeer znode children
        members            Verify consul members match kafka zone
        migrate            Migrate given topic partition to specified broker ids
        mirror             Continuously copy data between two Kafka clusters
        mount              A FUSE module to mount a Kafka cluster in the filesystem
        move               Move kafka partition from one dir to another
        offset             Manually set consumer group offset
        partition          Add partition num to a topic for better parallel
        peek               Peek kafka cluster messages ongoing from any offset
        ping               Ping liveness of all registered brokers in a zone
        produce            Produce a message to specified kafka topic
        sample             Java sample code of producer/consumer
        segment            Scan the kafka segments and display summary
        top                Unix “top” like utility for kafka topics
        topbroker          Unix “top” like utility for kafka brokers
        topics             Manage kafka topics
        topology           Print server topology and balancing stats of kafka clusters
        underreplicated    Display under-replicated partitions
        upgrade            Upgrade local gk to latest version
        verify             Verify pubsub clients synced with lagacy kafka
        webhook            Display kateway webhooks
        whois              Lookup PubSub App Information
        zkinstall          Install a zookeeper node on localhost
        zktop              Unix “top” like utility for ZooKeeper
        zones              Print zones defined in $HOME/.gafka.cf
        zookeeper          Monitor zone Zookeeper status by four letter word command

