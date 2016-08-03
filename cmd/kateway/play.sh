#!/bin/sh
#----------------------
# play with kateway
#----------------------

# register the demo cluster 'me'
zk create -p /kafka_pubsub
gk clusters -z local -add me -p /kafka_pubsub
gk clusters -z local -c me -s -public 1 -nickname me
echo '{"shard_stategy":"standard","timeout":10000000000,"idle_timeout":0,"max_idle_conns":5,"max_conns":20,"breaker":{"FailureAllowance":5,"RetryTimeout":10000000000},"pools":{"me":{"pool":"me","host":"127.0.0.1","port":"3306","user":"root","pass":"","db":"pubsub","charset":"utf8"}}}' | zk set -p /_kateway/jobcluster

# register a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9193/v1/topics/me/app1/foobar/v1?partitions=1&replicas=1'

# register a shadow topic
curl -i -XPOST -H'Appid: app14' -H'Subkey: mysubkey' http://localhost:9192/v1/guard/app1/foobar/v1/mygroup1

cd cmd/kateway/bench
make pub
make sub

# sub a topic
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1&reset=newest&limit=1'
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1'

# pub a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hhhhhhhello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d '@/Users/funky/gopkg/src/github.com/funkygao/fae/contrib/resources/dashboard.png' 'http://localhost:9191/v1/msgs/foobar/v1'

# raw sub
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9192/v1/raw/msgs/app1/foobar/v1?group=xx'

curl -XPUT -H'Appid: app2' -H'Subkey: mysubkey' -d '[{"partition":0,"offset":1221300}]' http://localhost:9192/v1/offsets/app1/foobar/v1/bench_go

# peek
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9192/v1/peek/app1/foobar/v1?n=10&wait=5s'

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

