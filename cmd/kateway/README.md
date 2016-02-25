# kateway

A fully-managed real-time RESTful Cloud Pub/Sub messaging service.

    _/    _/              _/
       _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/
      _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/
     _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/
    _/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/
                                                                               _/

### Features

- REST API
- http/https/websocket/http2 interface for Pub/Sub
- High performance, high throughput, low latency
  - over 100K message per second on a single host
  - graceful shudown without downtime
  - elastic scales
  - circuit breaker
- Service Discovery
  - self contained
- Realtime analytics and metrics monitor dashboard
- Communication can be one-to-many (fan-out), many-to-one (fan-in), and many-to-many
- Loosely coupled with kafka/zk
  - each component is replaceable
  - there is a storage abstraction layer
    - curretly 2 implementations
    - dummy
    - kafka
- Replicated storage and guaranteed at-least-once message delivery
- Ready for cloud
  - container ready
- [ ] Flexible delivery options
  - Both push- and pull-style subscriptions supported
- [ ] Quotas and rate limit, QoS
  - Flow control: Dynamic rate limiting 
- [ ] Plugins
  - authentication and authorization
  - transform
  - hooks
  - other stuff related to message-oriented middleware
- [ ] Encryption of all message data on the wire

### Ecosystem

- gk

  for backend kafka and kateway cluster management

- kateway

  Pub/Sub engine

- manager

  a multi-tenant web management console

- ehaproxy

  for elastic load balance of kateway


### Architecture

           +----+      +-----------------+          
           | DB |------|   manager UI    |
           +----+      +-----------------+                                                  
                               |                                                           
                               ^ register [application|topic|version|subscription]                       
                               |                                                          
                       +-----------------+                                                 
                       |  Application    |                                                
                       +-----------------+                                               
                               |                                                        
                               V                                                       
            PUB                |               SUB                                    
            +-------------------------------------+                                  
            |                                     |                                         
       HTTP |                                HTTP | keep-alive 
       POST |                                 GET | session sticky                        
            |                                     |                                      
        +------------+                      +------------+                 application 
     ---| PubEndpoint|----------------------| SubEndpoint|---------------------------- 
        +------------+           |          +------------+                     kateway
        | stateless  |        Plugins       | stateful   |                           
        +------------+  +----------------+  +------------+                          
        | quota      |  | Authentication |  | quota      |      
        +------------+  +----------------+  +------------+     
        | metrics    |  | Authorization  |  | metrics    |    
        +------------+  +----------------+  +------------+   
        | guard      |  | ......         |  | guard      |  
        +------------+  +----------------+  +------------+                      
        | registry   |                      | registry   |  
        +------------+                      +------------+                      
        | meta       |                      | meta       |  
        +------------+                      +------------+                      
            |                                     |    
            |    +----------------------+         |  
            |----| ZK or other ensemble |---------| 
            |    +----------------------+         |
            |                                     |    
            | Append                              | Fetch
            |                                     |                     
            |       +------------------+          |     
            |       |      Store       |          |    
            +-------+------------------+----------+   
                    |  kafka or else   |
           +----+   +------------------+        +---------------+
           | gk |---|     monitor      |--------| elastic scale |
           +----+   +------------------+        +---------------+


### Common scenarios

- Balancing workloads in network clusters
- Implementing asynchronous workflows
- Distributing event notifications
- Refreshing distributed caches
- Logging to multiple systems
- Data streaming from various processes or devices
- Reliability improvement

### FAQ

- how to batch messages in Pub?

  It is http client's job to put the variant length data into json array

- how to consume multiple messages in Sub?

  kateway uses chunked transfer encoding

- topic name cannot include
  - slash, dot, ~, +, @
  - case sensitive

### TODO

- [ ] kateway
  - (pubclient.go:22) cluster[gateway] closing kafka sync client: 6127
  - log remote addr and port
- [ ] manually reset metrics counter
- [ ] more strict validator for group and topic name
- [ ] POST will return 201 http status code
- [ ] ehaproxy integration with rsyslog test
- [ ] metrics flush/load test
- [ ] gzip sub response
- [ ] test pub/sub metrics load and flush
- [ ] online producers/consumers
- [ ] pub fails retry should be done at kafka pub sdk: sarama
- [ ] check hack pkg
- [ ] delayed pub
- [ ] message will have header
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

