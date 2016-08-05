# actor

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
    +------------------+       +------------------+     +------------------+
    | Actor/Controller |       | Actor/Controller |     | Actor/Controller |
    +------------------+       +------------------+     +------------------+
          Host1                      Host2                      HostN

### TODO

- [ ] any update/delete Job table need lock to avoid race condition with worker
- [ ]
