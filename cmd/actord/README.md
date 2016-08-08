# actord

### Architecture


    +----------+  +----------+  +----------+  +----------+  +----------+  +----------+
    | JobQueue |  | WebHook  |  | JobQueue |  | WebHook  |  | JobQueue |  | WebHook  |
    +----------+  +----------+  +----------+  +----------+  +----------+  +----------+
        |               |
        | 1:1           |
        |               |
    +-------------+  +-----------------+  
    | JobExecutor |  | WebHookExecutor |
    +-------------+  +-----------------+  
        |               |
        +---------------+
            |
            | 1:N
            |
    +--------------------+       +--------------------+     +--------------------+
    | Actor/Orchestrator |       | Actor/Orchestrator |     | Actor/Orchestrator |
    +--------------------+       +--------------------+     +--------------------+
          Host1                      Host2                      HostN
            |                          |                          |
            |     election             |                          |
            +-----------------------------------------------------+
                                     zone


### TODO

- [X] any update/delete Job table need lock to avoid race condition with worker
- [ ] watch orchestrator/jobconfig change, tables migration
- [X] metrics and alarm
- [ ] force rebalance
- [X] audit
- [ ] executor
  - learn from zabbix how to mv real time table to archive table
  - graceful shutdown
  - test dependent components outage
- [ ] manager
  - call CreateJobQueue
  - dashboard
  - browse history
