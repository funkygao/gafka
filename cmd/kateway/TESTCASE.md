# test cases

功能测试、压力测试、容错测试、疲劳测试、并发测试、edge case test

### pub

- pub ok, then shutdown kafka, then pub again
- shutdown alll kafka, start kateway, then pub, then startup 1node of 2-node kafka 
  cluster, then start another, then shutdown
- keepalive timeout, read/write timeout
- max message size, max header size
- keyed message
- when pub, what if shutdown kateway
- when refreshing meta store, no timeout, hang forever

### sub

- sub when partitions/brokers added/deleted

  fail, sub can recv the new partition message. but pub can produce to the new partitions

- sub a non-existent topic
  
  not allowed

- auto commit offset when kateway restarts

  fail

- start a sub, then another... then shutdown...

- never get message lost or duplicated

- stop kafka when sub is running

### zk

- use iptables to simulate zk session timeout

### haproxy redispatch and soft reload

#### redispatch
1. launch 2 kateways (k1, k2)
2. create suitable config file to dispatch sub reqs to these 2 kateways
3. launch haproxy with this cfg
4. send sub req from client
5. find the target kateway(k1) which will handle this client(balance by source ip) req from kateway.log
6. using client to send pub POST reqs one by one
7. using client to send sub GET reqs one by one simutaneously 
8. observe pub/sub client resp result(everything is ok, pub Status Code: 201, sub Status Code: 200)
9. shutdown target kateway k1 by using `kill -2 K_1_PID`
10. observe pub/sub client behavior
	expected hehavior:
	* pub client hang up
	* sub client hang up or get 204 Status Code

	bug behavior:
	* pub client get 504 Status Code
	* sub client get 504 Status Code

11. after 20 seconds, pub/sub client continue running, get subsequent resp(everything is still ok)
12. over


#### soft reload
1. launch 1 kateway
2. create suitable config file (.haproxy.reload.test.cfg) to dispatch sub req to this kateway and only launch one haproxy process by removing "stats bind-process 4", "nbproc 4", listen 127.0.0.1:{{.Port}} section from formal gafka haproxy config file
3. launch haproxy using "haproxy -f .haproxy.reload.test.cfg", the haproxy process will be HA_1_PID
4. using client to send pub POST reqs one by one
5. using client to send sub GET reqs one by one simutaneously
6. try to soft reload haproxy by using "haproxy -f .haproxy.reload.test.cfg -sf HA_1_PID"
7. observe pub/sub client behavior
	expexcted behavior:
	* pub client continue running, get sub sequent resp(seamless switch)
	* pub client continue running, get sub sequent resp(seamless switch)

	bug behavior:
	* sub client get 400 Status Code ("consumers more than available partitions")

### misc

- graceful shutdown
- simulate bad network environment and slow client for testing
- logging what should be logged
- kill -9 client


### bugs

- sub after 1m, got no 204 reply: empty reply
- when pub a too big msg, fasthttp has empty reply while normal http is ok
- when kateway shutdown, it did not flush offset commit
- guard.go panic
