# gafka 

A full ecosystem that is built around kafka powered by golang.

### The Whole Picture

                +-----------+
                | VirtualIP |
                +-----------+                                
                      |
              +--------------+                                 Alarm         Dashboard
              |              |                                    |             |              gk
     +-------------------------------------------------------------------------------------------+
     |        |              |                                    |             |                |
     |  +----------+    +----------+                              |             |                |
     |  | ehaproxy |    | ehaproxy |                              |             |                |
     |  +----------+    +----------+                              |             |                |
     |      |                |  | discovery                       |             |                |
     |      +----------------+  |                                 +-------------+                |
     |            | LB          |                                        |                       |
     |            |             |   +--------------------+           +--------+                  |
     |            |             +---|                    | election  |        | watch            |
     |            |                 | zookeeper ensemble |-----------| kguard |-------------+    |
     |            |             +---|                    |           |        | aggragator  |    |
     |            |             |   +--------------------+           +--------+             |    |
     |            |     +-------+           |                            |                  |    |
     |            |     | registry          | orchestration             SOS                 |    |      +- Pub
     |      +---------------+               |-----------+                      +---------+  |    | REST |
     |      |               |               |           |                      | kateway |--|----|------|
     |  +---------+    +---------+      +--------+    +--------+               +---------+  |    |      |
     |  | kateway |    | kateway |      | actord |    | actord |                            |    |      +- Sub
     |  +---------+    +---------+      +--------+    +--------+                            |    |
     |                      |               | executor                                      |    |    
     |                      |            +--------------+                                   |    |   
     |                      |            |              |                                   |    |  
     |                      |       +---------+    +---------+  push                        |    |  
     |             +--------+       | JobTube |    | Webhook |------------------------------|----|---Endpoints
     |             |        |       +---------+    +---------+                              |    |
     |             |        |           | scheduler     | sub                               |    |
     |        auth |        |job WAL    | dispatch      |                                   |    |
     |      +------+        +---------------------------+------------------+                |    |
     |      |               | tenant shard              | pubsub           | flush          |    |
     |  +----------+    +---------+                 +-------+           +------+            |    |
     |  | auth DB  |    | DB Farm |                 | kafka |           | TSDB |            |    |
     |  +----------+    +---------+                 +-------+           +------+            |    |
     |      |               |                           |                  |                |    |   
     |      |               +----------------------------------------------+                |    |  
     |      |                                   |                                           |    | 
     |      |                                   +-------------------------------------------+    |
     |      |                                                                                    |  
     |      |                                                                               zone |   
     +-------------------------------------------------------------------------------------------+
            |
        WebConsole 

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

- [kguard](https://github.com/funkygao/gafka/tree/master/cmd/kguard)

  Kafka clusters body guard that emits health info to InfluxDB and exports key warnings to zabbix for alarming.

### Install

    export PATH=$PATH:$GOPATH/bin

    #========================================
    # install go-bindata first
    #========================================
    go install github.com/jteeuwen/go-bindata/go-bindata

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
  - 0.6Million message per second

