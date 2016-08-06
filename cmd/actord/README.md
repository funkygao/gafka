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
            |     election             |                          |
            +-----------------------------------------------------+
                                     zone


### Monitor

- [ ] topology
- [ ] work load assignment
- [ ] dead workers

### TODO

- [X] any update/delete Job table need lock to avoid race condition with worker
- [ ] app tables migration
- [ ] metrics
- [ ] worker
  - learn from zabbix how to mv real time table to archive table
  - graceful shutdown
  - test errors
- [ ] manager
  - call CreateJobQueue
  - dashboard
  - browse history
- [ ] gk job
  - list due jobs
  - list real-time/archive job count
