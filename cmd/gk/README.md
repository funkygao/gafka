# gk

Unified multi-datacenter multi-cluster kafka swiss-knife management console.

Currently gk manages distributed clusters of:

- zookeeper
- kafka
- redis
- haproxy
- kateway
- kguard
- actord

### Usage

    $gk
    Unified multi-datacenter multi-cluster kafka swiss-knife management console
    
    usage: gk [--version] [--help] <command> [<args>]
    
    Available commands are:
    ?                  FAQ
    agent              Starts the gk agent daemon TODO
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
    histogram          Histogram of kafka produced messages and network traffic
    job                Display job/actor related znodes for PubSub system.
    kateway            List/Config online kateway instances
    kguard             List online kguard instances
    lags               Display online high level consumers lag on a topic
    logstash           Sample configuration for logstash
    lszk               List kafka related zookeepeer znode children
    members            Verify consul members match kafka zone
    migrate            Migrate given topic partition to specified broker ids
    mirror             Continuously copy data between two remote Kafka clusters
    mount              A FUSE module to mount a Kafka cluster in the filesystem
    move               Move kafka partition from one dir to another
    offset             Manually set consumer group offset
    partition          Add partition num to a topic for better parallel
    peek               Peek kafka cluster messages ongoing from any offset
    perf               Probe system low level performance problems with perf
    ping               Ping liveness of all registered brokers in a zone
    produce            Produce a message to specified kafka topic
    rebalance          Restore the leadership balance for a given topic partition
    redis              Monitor redis instances
    sample             Java sample code of producer/consumer
    segment            Scan the kafka segments and display summary
    sniff              Sniff traffic on a network with libpcap
    time               Parse Unix timestamp to human readable time
    top                Unix “top” like utility for kafka topics
    topbroker          Unix “top” like utility for kafka brokers
    topics             Manage kafka topics
    topology           Print server topology and balancing stats of kafka clusters
    underreplicated    Display under-replicated partitions
    upgrade            Upgrade local gk tools to latest version
    verify             Verify pubsub clients synced with lagacy kafka
    webhook            Display kateway webhooks TODO
    whois              Lookup PubSub App Information
    zk                 Monitor zone Zookeeper status by four letter word command
    zkinstall          Install a zookeeper node on localhost
    zones              Print zones defined in $HOME/.gafka.cf
