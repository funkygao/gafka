# kateway

A RESTful gateway for kafka that supports Pub and Sub.

    _/    _/              _/
       _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/
      _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/
     _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/
    _/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/
                                                                               _/

### Features

- http/https and websocket supported
- Quotas and rate limit
- Circuit breakers
- Plugins
  - authentication and authorization
  - transform
- Elastic scales
- Analytics
- Monitor Performance
- Service Discovery
- Hot reload without downtime
- Audit
- Distributed load balancing
- Health checks

### Architecture

           +----+      +-----------------+          
           | DB |------|   manager UI    |
           +----+      +-----------------+                                                  
                               |                                                           
                               ^ register [application|topic|binding]                       
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
        +------------+                      +------------+          application border  
     ---| PubEndpoint|----------------------| SubEndpoint|---------------------------- 
        +------------+           |          +------------+                            
        | stateless  |        Plugins       | stateful   |                           
        +------------+  +----------------+  +------------+                          
        | monitor    |  | Authentication |  | monitor    |                         
        +------------+  +----------------+  +------------+                        
        | guard      |                      | guard      |                       
        +------------+                      +------------+                      
            |                                     |    
            |    +----------------------+         |  
            |----| ZK or other ensemble |---------| 
            |    +----------------------+         |
            |                                     |    
            | Pub                                 | Fetch
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
                       +-----------------------------------------+        
                                           |
                                           | HTTP/1.1 keep-alive
                                           |     
                                    +--------------+                         +----------+
                                    | LoadBalancer |                         | registry |
                                    +--------------+                         +----------+
                                           |
                                           | HTTP/1.1 keep-alive             +----------+
                                           |      session sticky             | guard    |
                       +-----------------------------------------+           +----------+
                       |              |            |             |      
                  +---------+   +---------+   +---------+   +---------+      +----------+
                  | Kateway |   | Kateway |   | Kateway |   | Kateway |      | ensemble |
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

- how to scale kateway?

### TODO

- [ ] check hack pkg
- [ ] https, outer ip must https
- [ ] guard.go panic
- [ ] Pub HTTP POST Request compress
- [ ] metrics
  - intranet/extranet pub
  - failure count
  - consumer performance: how many msges can it consume
  - rank by topic/ver/app
- [ ] for intranet traffic, record the src ip and port
- [ ] delayed pub
- [ ] https://github.com/corneldamian/httpway
- [ ] warmup
- [ ] elastic scale
- [ ] pool benchmark
- [ ] sub lock more precise 
- [ ] Update to glibc 2.20 or higher
- [ ] compression in kafka
- [ ] plugin
- [ ] pub audit
- [ ] smooth upgrade kafka version

