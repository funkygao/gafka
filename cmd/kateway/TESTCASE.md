# test cases

功能测试、压力测试、容错测试、疲劳测试、并发测试、edge case test

### pub

- pub ok, then shutdown kafka, then pub again
- shutdown alll kafka, start kateway, then pub, then startup 1node of 2-node kafka 
  cluster, then start another, then shutdown
- keepalive timeout, read/write timeout
- max message size, max header size

### sub

- sub a non-existent topic


### misc

- graceful shutdown
