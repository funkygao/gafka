# psubd

A REST Proxy for kafka that supports both Pub and Sub.

### TODO

- [ ] kafka conn pool
- [ ] rate limit
- [ ] metrics report
- [ ] mem pool 
- [ ] consumer groups
- [ ] profiler
- [ ] Update to glibc 2.20 or higher

### EdgeCase

- when producing/consuming, partition added
- when producing/consuming, brokers added/died



post("/{group}")  create group
post("/{group}/instances/{instance}/offsets") 
delete("/{group}/instances/{instance}")
get("/{group}/instances/{instance}/topics/{topic}")
