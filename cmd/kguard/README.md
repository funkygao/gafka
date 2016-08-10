# kguard

Kafka clusters body guard that emits health info to InfluxDB.

### Architecture

    
                       👥 ✉  here I am
                        |              
        +------------------------+------------------------------------------------+
        | alert                  | dashboard                                alert |
        | (aggregated)           | Grafana                            (threshold) |
        |               +-----------------+                                       |
        |               |                 |                                       |
    +--------+     +----------+      +----------+    +---------+               +--------+
    | zabbix |     | InfluxDB |      | OpenTSDB |    | TSDB... |               | zabbix |- OS level monitor
    +--------+     +----------+      +----------+    +---------+               +--------+
        |            |                   |                  |                     |
        |            +--------------------------------------+                 zabbix agents
        |                      ^
        | periodically         |
        | call                 | flush
        | RESTful              |
        |                 +-----------+
        |                 | telemetry |
        |                 +-----------+                                        
        |                      | collect                                       +- external scripts
        |                      V                                               |- kguard floating
        |     +-------------------+                                            |- ehaproxy
        |     | in-memory metrics |                                            |- actord
        |     +-------------------+                                            |- swf
        |       ^               ^                                              |- F5 latency
        |       | read          | write                                        |- zone wide servers
        |       |            +----------------------+                          |- influx query
        V       |            |                      |                          |- influxdb server
    +-------------+   +--------------+   +----------------------------+ plugin |- kateway/pubsub
    | HTTP server |   | SOS receiver |   | Watchers/MonitorAggregator |--------|- kafka
    +-------------+   +--------------+   +----------------------------+        |- zookeeper 
        |                    |                      |                          +- ...
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

