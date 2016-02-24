# MQTT QoS 2

    client                  server
      |                        |
      | pub                    |
      |----------------------->|
      |                        | 1. server will not dispatch msg, it will wait for client REL(release) msg
      |                        | 2. if client did not recv REC(received), it will retry
      |                        | 3. server generate msg id, and sent it back with REC
      |            id received | 4. server put the msg into buffer 
      |<-----------------------|
      |                        | 1. server will get msg from buffer and dispatch
      |                        | 2. if server not recv REL, the msg will finally be GCed
      |                        | 3. server might recv dup REL, but the id is uniq
      | release id             | 4. the msg id might not be found in buffer
      |----------------------->|
      |                        |
      |                        | 1. client will retry REL until it get COMP
      |                        | 2. server might send dup COMP to client
      |               complete |
      |<-----------------------|
      |                        |
