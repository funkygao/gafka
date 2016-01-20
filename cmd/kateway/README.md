# kateway

Kafka as a service

A RESTful gateway for kafka that supports Pub and Sub.

Don't make programmer think!

    _/    _/              _/
       _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/
      _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/
     _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/
    _/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/
                                                                               _/

### Features

- http/https/websocket/http2 interface for programmer
- High performance and high throughput
  - over 100K message per second on a single host
- Loosely coupled with kafka/zk
  - each component is replaceable
- Service Discovery
  - self contained
- Quotas and rate limit, QoS
- Circuit breakers
- Plugins
  - authentication and authorization
  - transform
  - other stuff related to enterprise message bus
- Elastic scales
- Works closely with the powerful tool: gk
- Realtime analytics and metrics monitor
- Multi-tenant message metrics dashboard
- Graceful shutdown without downtime
- Handles most of the difficult stuff while providing easy to use interface

### Befinits

- export message tube to internet
- let programmers forget about servers and concentrate on services/kateway
  - no zk
  - no kafka
  - no topology
  - a single endpoint
- even benificial for OPS team of kafka/zk
  - single client
  - easy upgrade/maintain
  - consolidated hooks possible

### Ecosystem

- gk

  for backend kafka

- kateway

  pubsub endpoint for programmers

- elasticha

  for elastic load balance of kateway


### Architecture

           +----+      +-----------------+          
           | DB |------|   manager UI    |
           +----+      +-----------------+                                                  
                               |                                                           
                               ^ register [application|topic|pub|sub]                       
                               |                                                          
                       +-----------------+                                                 
                       |  Application    |                                                
                       +-----------------+                                               
                               |                                                        
                               V                                                       
            PUB                |               SUB                                    
            +-------------------------------------+                                  
            |                                     |                                         
       HTTP |                                HTTP |                                        
       POST |                                 GET |                                       
            |                                     |                                      
        +------------+                      +------------+                 application 
     ---| PubEndpoint|----------------------| SubEndpoint|---------------------------- 
        +------------+           |          +------------+                     kateway
        | stateless  |        Plugins       | stateful   |                           
        +------------+  +----------------+  +------------+                          
        | quota      |  | Authentication |  | quota      |                         
        +------------+  +----------------+  +------------+                          
        | monitor    |  | Authorization  |  | monitor    |                         
        +------------+  +----------------+  +------------+                        
        | guard      |                      | guard      |                       
        +------------+                      +------------+                      
            |                                     |    
            |    +----------------------+         |  
            |----| ZK or other ensemble |---------| 
            |    +----------------------+         |
            |                                     |    
            | Put                                 | Get
            |                                     |                     
            |       +------------------+          |     
            |       |      Store       |          |    
            +-------+------------------+----------+   
                    |  kafka or else   |
                    +------------------+        +-----------------------------+
                    |     monitor      |--------| elastic partitioner/monitor |
                    +------------------+        +-----------------------------+


### Deployment

                  +---------+   +---------+   +---------+   +---------+      
                  | client  |   | client  |   | client  |   | client  |     
                  +---------+   +---------+   +---------+   +---------+    
                       |              |            |             |
                       +-----------------------------------------+           +----------+
                                           |                                 | elasticha|
                                           | HTTP/1.1 keep-alive             +----------+
                                           |     
                                    +--------------+                         +----------+
                                    | ehaproxy     |                         | registry |
                                    +--------------+                         +----------+
                                           |
                                           | HTTP/1.1 keep-alive             +----------+
                                           |      session sticky             | guard    |
                       +-----------------------------------------+           +----------+
                       |              |            |             |      
                  +---------+   +---------+   +---------+   +---------+      +----------+
                  | Kateway |---| Kateway |---| Kateway |---| Kateway |------| ensemble |
                  +---------+   +---------+   +---------+   +---------+      +----------+
                       |              |            |             |
                       +-----------------------------------------+           +----------+
                                            |                                | metrics  |
                             +---------------------------+                   +----------+
                             |              |            |             
                         +---------+   +---------+   +---------+   
                         | Store   |   | Store   |   | Store   |
                         +---------+   +---------+   +---------+   

### FAQ

- how to batch messages in Pub?

  It is http client's job to put the variant length data into json array

- how to consume multiple messages in Sub?

  kateway uses chunked transfer encoding

- how to load balance the Pub?

  - client side LB
  - haproxy

- how to load balance the Sub?

  haproxy MUST enable session sticky

- topic name cannot include
  - slash, dot, ~, +, @
  - case sensitive

### TODO

- [ ] ehaproxy integration with rsyslog test
- [ ] metrics flush/load test
- [ ] gzip sub response
- [ ] test pub/sub metrics load and flush
- [ ] online producers/consumers
- [ ] pub fails retry should be done at kafka pub sdk: sarama
- [ ] check hack pkg
- [ ] delayed pub
- [ ] warmup
- [ ] https, outer ip must https
- [ ] Pub HTTP POST Request compress
- [ ] metrics
- [ ] who post the most messages
- [ ] for intranet traffic, record the src ip and port
- [ ] https://github.com/corneldamian/httpway
- [ ] Update to glibc 2.20 or higher
- [ ] compression in kafka
- [ ] plugin
- [ ] pub audit

