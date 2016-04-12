# gitlab event tools

It is integrated with gitlab system hooks.

On all gitlab projects git push, gitlab will invoke this hook and gitlabfeed
will send this event to PubSub system.


### [webhookd](https://github.com/funkygao/gafka/tree/master/cmd/gitlabtool/webhookd.go)

Webhookd is a webhook endpoint that is integrated with gitlab.
It accepts events from gitlab hook and send it to pubsub system.

### [gitop](https://github.com/funkygao/gafka/tree/master/cmd/gitlabtool/gitop)

gitlab top.

Same operation mode with [tig](https://github.com/jonas/tig) except:
- it support multiple repositories
- it consumes events from kafka and on each gitlab event, it displays on top

