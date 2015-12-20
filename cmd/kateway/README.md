# kateway

A REST gateway for kafka that supports Pub and Sub.

### Features

- RESTful API for kafka Pub/Sub
- http/https and websocket supported
- Quotas and rate limit
- Plugins
  - authentication and authorization
  - transform
- Elastic scales
- Analytics
- Monitor Performance
- Service Discovery
- Circuit breakers
- Hot reload without downtime
- Audit
- Distributed load balancing
- Health checks
- Web UI to monitor lag

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
                                    +--------------+
                                    | LoadBalancer |
                                    +--------------+
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

### Usage

    pub:
    curl -i -XPOST -H "Pubkey: mypubkey" -d '{"orderId":56, "uid": 89}' "http://localhost:9191/topics/v2/foobar?key=56"

    sub:
    curl -i XGET -H "Subkey: mysubkey" http://localhost:9192/topics/v2/foobar/mygroup1?limit=100

### FAQ

- how to batch messages in Pub?

  It is http client's job to put the variant length data into json array

- how to consume multiple messages in Sub?

  kateway uses chunked transfer encoding

- how to load balance the Sub?

  haproxy MUST enable session sticky

- how to scale kateway?

### TODO

- [ ] delayed pub
- [ ] https://github.com/corneldamian/httpway
- [X] multi-cluster
- [ ] warmup
- [ ] elastic scale
- [ ] Under high load presure, mpool pkg might become a burden
- [ ] pool benchmark
- [ ] sub lock more precise 
- [ ] registers itself in consul
- [ ] async pub not fully tested
- [ ] Update to glibc 2.20 or higher
- [ ] compression in kafka
- [ ] plugin

### Test

- [ ] stress test and leakage test
- [ ] simulate bad network environment for testing
- [ ] message never get lost
- [ ] post a BIG msg
- [ ] logging

### Bugs

- [ ] race conditions
- [X] pub listener closes silently in high load case

### EdgeCase

- when producing/consuming, partition added
- when producing/consuming, brokers added/died

    1. a sub client -> kateway
    2. consumes msg 1-10
    3. client disconnects 
    4. kateway fails to write(msg 11), and kills this client record(10s)
    5. will commit offset to 10

    1. a sub client -> kateway
    2. no msgs and reach idle max timeout, kateway closes the client
    3. we MUST handle this case

