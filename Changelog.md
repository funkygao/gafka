# gafka Changelog

## kateway

### 0.3 - 2016-09-26

**Upgrade from 0.2.6**: APIs for manager contain backwards incompatible changes.

This is the first major release that is stable for production environment.

Features:

* [#4](https://github.com/funkygao/gafka/issues/4) - job(schedulable message)
* [#5(https://github.com/funkygao/gafka/issues/5) - hinted handoff
* [#6](https://github.com/funkygao/gafka/issues/6) - webhook
* [#7](https://github.com/funkygao/gafka/issues/7) - avro message schema registry
* [#8](https://github.com/funkygao/gafka/issues/8) - server side message filter: message tag
* [#9](https://github.com/funkygao/gafka/issues/9) - display real consumer client ip instead of the kateway instance ip 
* swagger
* introduced a new daemon: actord

Enhancements:

* correctly handles the 5XX and 4XX error response
* haproxy/kateway provides unified error response
* benchmark compare
* handles zk session expire problem
* fixed a lot of race conditions
* bad Pub/Sub client rate limit
* message tag has no extra overhead: no mem copy/move
* display both proxy and real remote ip of the Sub client
* fixed potential memory/goroutine leak
* dynamic gzip on demand
* pub/sub audit
* solve the IPv6 remote ip problem
* support CORS
* use last ok state if manager/ehaproxy/meta encounter suspectable problems during refresh

WIP:

* shadow topic
* tagged metrics
* kateway as RM for 2PC

## kguard

kguard = SOS + watchers + monitor

It is refactored to be plugin architecture. Easy to add new plugins.
