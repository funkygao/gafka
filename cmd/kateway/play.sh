#!/bin/sh
#----------------------
# play with kateway
#----------------------

# register the demo cluster 'me'
zk create -p /kafka_pubsub
gk clusters -z local -add me -p /kafka_pubsub
gk clusters -z local -c me -s -public 1 -nickname me
zk create -p /_kateway/orchestrator/jobconfig
zk create -p /_kateway/orchestrator/webhooks
echo '{"shard_stategy":"standard","timeout":10000000000,"global_pools":{"ShardLookup":true},"idle_timeout":14400000000000,"max_idle_conns":5,"max_conns":50,"breaker":{"FailureAllowance":10,"RetryTimeout":5000000000},"pools":{"AppShard1":{"pool":"AppShard1","host":"127.0.0.1","port":"3306","user":"root","pass":"","db":"pubsub","charset":"utf8"},"ShardLookup":{"pool":"ShardLookup","host":"127.0.0.1","port":"3306","user":"root","pass":"","db":"pubsub","charset":"utf8"}},"cache_store":"mem","cache_cap":1024,"cache_keyhash":false,"lookup_cache_max_items":1048576,"lookup_pool":"ShardLookup","default_lookup_table":"AppLookup"}' | zk set -p /_kateway/orchestrator/jobconfig
# the jobconfig is created by fae/config, go test -v

# register a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9193/v1/topics/app1/foobar/v1?partitions=1&replicas=1'

# register a job
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9193/v1/jobs/app1/foobar/v1'

# register a webhook
curl -XPUT -H'Appid: app1' -H'Pubkey: mypubkey' -d '{"endpoints":["http://localhost:9876"]}' 'http://localhost:9193/v1/webhooks/app1/foobar/v1'

# auth token
curl -XGET -H'X-App-Id: app1' -H'X-App-Secret: helllo' 'http://localhost:9191/v1/auth'

# register a shadow topic
curl -i -XPOST -H'Appid: app14' -H'Subkey: mysubkey' http://localhost:9192/v1/guard/app1/foobar/v1/mygroup1

cd cmd/kateway/bench
make pub
make sub

# pub a job
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hhhhhhhello world!' 'http://localhost:9191/v1/jobs/foobar/v1?delay=20'
# del a job
curl -XDELETE -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9191/v1/jobs/foobar/v1?id=341659367487049728'

# pub a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hhhhhhhello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d '@/Users/funky/gopkg/src/github.com/funkygao/fae/contrib/resources/dashboard.png' 'http://localhost:9191/v1/msgs/foobar/v1'

# sub a topic
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1&reset=newest&limit=1'
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1'

# raw sub
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9193/v1/raw/msgs/app1/foobar/v1?group=xx'

# reset offset
curl -XPUT -H'Appid: app2' -H'Subkey: mysubkey' -d '[{"partition":0,"offset":1221300}]' http://localhost:9192/v1/offsets/app1/foobar/v1/bench_go

# peek
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9193/v1/peek/app1/foobar/v1?n=10&wait=5s'

# sub status
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9193/v1/status/app1/foobar/v1?group=group1'

# subd status
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9193/v1/subd/foobar/v1'

# delete a sub group
curl -XDELETE -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9193/v1/groups/app1/foobar/v1/group1'


# pprof debug 
curl http://localhost:9194/debug/pprof/


#----------------------
# play with meta
#----------------------
curl http://localhost:9193/v1/status
echo
curl http://localhost:9193/v1/clusters
echo


#----------------------
# manually pub/sub
#----------------------
telnet localhost 9191
POST /v1/msgs/foobar/v1 HTTP/1.1
Host: localhost
Appid: app1
Pubkey: app1
Content-Length: 1

a

telnet localhost 9192
GET /v1/msgs/app1/foobar/v1?group=group1 HTTP/1.1
Host: localhost
Appid: app2
Subkey: key

