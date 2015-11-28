# kateway

A REST gateway for kafka that supports both Pub and Sub.

### Features

- RESTful API for kafka Pub/Sub
- Quotas and rate limit
- Plugable Authentication and Authorization
- Analytics
- Monitor Performance
- Service Discovery
- Circuit breakers
- Multiple versioning topic
- Hot reload without downtime
- Transforms on the fly
- Audit
- Distributed load balancing
- Health checks

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


### Usage

    pub:
    curl -i -XPOST -H "Pubkey: mypubkey" -d 'm=hello world' "http://localhost:9090/v1/topics/foobar?ack=2&timeout=1&retry=3"

    sub:
    curl -i http://localhost:9090/v1/topics/foobar/group1?limit=10000000

### TODO

- [ ] client can query lag
- [ ] websocket for pub/sub
- [ ] sub metrics report
- [ ] mem pool 
- [ ] profiler
- [ ] Update to glibc 2.20 or higher
- [ ] pub/sub support config passed in
- [ ] graceful shutdown
- [X] influxdb tag
- [ ] compression in kafka

### Bugs

- [ ] /usr/local/go/src/net/http/server.go:1934: http: multiple response.WriteHeader calls
- [ ] panic: sync: negative WaitGroup counter

### EdgeCase

- when producing/consuming, partition added
- when producing/consuming, brokers added/died


post("/{group}")  create group
post("/{group}/instances/{instance}/offsets") 
delete("/{group}/instances/{instance}")
get("/{group}/instances/{instance}/topics/{topic}")
