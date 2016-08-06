# actord

### Architecture


    +----------+  +----------+  +----------+  +----------+  +----------+  +----------+
    | JobQueue |  | JobQueue |  | JobQueue |  | JobQueue |  | JobQueue |  | JobQueue |
    +----------+  +----------+  +----------+  +----------+  +----------+  +----------+
        |               |
        | 1:1           |
        |               |
    +----------+  +----------+  
    |  Worker  |  |  Worker  |
    +----------+  +----------+  
        |             |
        +-------------+
            |
            | 1:N
            |
    +------------------+       +------------------+     +------------------+
    | Actor/Controller |       | Actor/Controller |     | Actor/Controller |
    +------------------+       +------------------+     +------------------+
          Host1                      Host2                      HostN
            |                          |                          |
            +-----------------------------------------------------+
                                     zone


### Monitor

- [ ] topology
- [ ] work load assignment
- [ ] dead workers

### TODO

- [ ] any update/delete Job table need lock to avoid race condition with worker
