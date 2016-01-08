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
