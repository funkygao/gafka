# psubd

A REST Proxy for kafka that supports both Pub and Sub.


### Architecture

                       +-----------------+          binding
                       | maas manager UI |--------------------------->----------------------+
                       +-----------------+                                                  |
                               |                                                            |
                               ^ register [application|topic|binding]                       |
                               |                                                            |
                       +-----------------+                                                  |
                       |  Application    |                                                  |
                       +-----------------+                                                  |
                               |                                                            |
                               V                                                            |
            PUB                |               SUB                                          |
            +-------------------------------------+                                         |
            |                                     |                                         |
       HTTP | pubkey                         HTTP | subkey                                  |
       POST | secret                          GET |                                         | binding
            |                                     |--+ batchSize                            | event
            | Header: topic.id                    |  | Optional: topic                      |
            | Header: key                         |  | Optional: offset                     |
            | Header: acks                        |  | timeout                              |
            | Body: payload                       |  | timeout                              |
        +------------+                      +------------+          application border      |
     ---| PubEndpoint|----------------------| SubEndpoint|----------------------------      |
        +------------+                      +------------+                                  |
        | stateless  |                      | stateful   |                                  V
        +------------+                      +------------+                                  |
        | monitor    |                      | monitor    |                                  |
        +------------+                      +------------+                                  |
            |                                     |     |                                   |
            | Producer                   Consumer |     |                                   |
            |                            Group    |     +---------------+                   |
            |                                     |                     |                   |
            |       +------------------+          |     +----------------------+            |
            |       |  Storage Cluster |          |     | ZK or alike ensemble |-----<------+
            +-------+------------------+----------+     +----------------------+
                    |  kafka or else   |
                    +------------------+        +---------------------+
                    |     monitor      |--------| elastic partitioner |
                    +------------------+        +---------------------+

### TODO

- [ ] kafka conn pool
- [ ] rate limit
- [ ] metrics report
- [ ] mem pool 
- [ ] consumer groups
- [ ] profiler
- [ ] Update to glibc 2.20 or higher
- [ ] consumer id discard
- [ ] bug: same consumer id get copy of messages. If client conn broken, stop the consumer group

### EdgeCase

- when producing/consuming, partition added
- when producing/consuming, brokers added/died



post("/{group}")  create group
post("/{group}/instances/{instance}/offsets") 
delete("/{group}/instances/{instance}")
get("/{group}/instances/{instance}/topics/{topic}")
