# kateway

A fully-managed real-time secure and reliable RESTful Cloud Pub/Sub streaming message service.

    _/    _/              _/
       _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/
      _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/
     _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/
    _/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/
                                                                               _/

### Alternatives

- google Cloud Pub/Sub
- amazon kenesis
- misc
  - pubnub
  - pusher
  - firebase
  - parse

### Features

- REST API
  - http/https/websocket/http2 interface for Pub/Sub
- Systemic Quality Requirements
  - Performance & Throughput
    - > 100K msg/sec delivery on a single host
  - Scalability
    - scales to 1M msg/sec
    - elastic scales
  - Latency
    - < 1s delivery
  - Availability
    - Graceful shutdown without downtime
  - Graceful Degrade
    - throttle
    - circuit breaker
  - Rich monitoring and alarm 
- Fully-managed
  - Discovery
  - Create versioned topics, subscribe to topics
  - Dedicated real-time metrics and fully-functional dashboard 
  - Easy trouble shooting
  - [ ] Managed integration service via Webhooks
  - [ ] Visualize message flow
- Communication can be 
  - one-to-many (fan-out)
  - many-to-one (fan-in)
  - many-to-many
- Replicated storage and guaranteed at-least-once message delivery
- Flexible delivery options
  - Both push- and pull-style subscriptions supported
- Enables sophisticated streaming data processing
  - because one app may emit kateway stream data into another kateway stream
- [ ] Quotas and rate limit, QoS
  - Flow control: Dynamic rate limiting 
- [ ] Plugins
  - authentication and authorization
  - transform
  - hooks
  - other stuff related to message-oriented middleware
- [ ] Encryption of all message data on the wire


### Common scenarios

- Balancing workloads in network clusters
- Implementing asynchronous workflows
- Distributing event notifications
- Refreshing distributed caches
- Logging to multiple systems
- Data streaming from various processes or devices
- Reliability improvement

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



### FAQ

- why named kateway?

  Admittedly, it is not a good name. Just short for kafka gateway.

- how to batch messages in Pub?

  It is http client's job to put the variant length data into json array

- how to consume multiple messages in Sub?

  kateway uses chunked transfer encoding

- http header size limit?

  4KB

- what is limit of a pub message in size?

  1MB

- can I pub an empty message?

  No

- if sub with no arriving message, how long do client get http 204?

  30s

- after I subscribe a topic on manager ui, why my client got http 401?

  yes, this is a trade off. You have to wait 5 minutes.

### TODO

- [ ] data needs to be enriched/sanitized before being consumed
- [ ] check hack pkg
- [ ] SLA of message retention
- [ ] delayed pub
- [ ] https, outer ip must https
- [ ] https://github.com/corneldamian/httpway
- [ ] Update to glibc 2.20 or higher
- [ ] compress
  - gzip sub response
  - Pub HTTP POST Request compress
  - compression in kafka
