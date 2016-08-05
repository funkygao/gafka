# kguard

Kafka clusters body guard that emits health info to InfluxDB.

### Architecture

    
                       👥 ✉  here I am
                        |              
        +------------------------+
        | alarm                  | dashboard
        |               +-----------------+
        |               |                 |
    +--------+     +----------+      +----------+    +---------+
    | zabbix |     | InfluxDB |      | OpenTSDB |    | TSDB... |
    +--------+     +----------+      +----------+    +---------+
        |            |                   |                  |
        |            +--------------------------------------+
        |                      ^
        | periodically         |
        | call                 | flush
        | RESTful              |
        |                 +------------+
        |                 | telementry |
        |                 +------------+
        |                      | collect
        |                      V
        |     +-------------------+
        |     | in-memory metrics |
        |     +-------------------+                                      +- external scripts(plugin)
        |       ^               ^                                        |- F5 latency
        |       | read          | write                                  |- zone wide servers
        |       |            +----------------------+                    |- influx query
        V       |            |                      |                    |- influxdb server
    +-------------+   +--------------+   +----------------------------+  |- kateway/pubsub
    | HTTP server |   | SOS receiver |   | Watchers/MonitorAggregator |--|- kafka
    +-------------+   +--------------+   +----------------------------+  |- zookeeper 
        |                    |                      |                    +- ...
        +-------------------------------------------+
                             | contains
                     +---------------+          +---------------------+     +---------------------+
                     | kguard leader |          | kguard hot standby1 |     | kguard hot standby2 | 
                     +---------------+          +---------------------+     +---------------------+
                             |                          |                       |
                             +--------------------------------------------------+
                                                  | election
                                          +--------------------+
                                          | zookeeper ensemble |
                                          +--------------------+

