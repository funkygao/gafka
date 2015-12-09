#!/bin/sh
#----------------------
# play with kateway
#----------------------

# initialize executor reserved topic
gk topics -z local -c me -add _executor._kateway.v1 -replicas 1

# register a topic
curl -XPOST -H'Appid: _executor' -H'Pubkey: mypubkey' -d'{"cmd":"createTopic", "topic": "foobar", "appid": "app1", "ver": "v1"}' http://localhost:9191/topics/_kateway/v1

# sub a topic
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/topics/app1/foobar/v1/group1?reset=newest&limit=1'

# raw sub
curl -XGET http://localhost:9192/raw/topics/app1/foobar/v1

# pub a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/topics/foobar/v1'



#----------------------
# play with meta
#----------------------
curl http://localhost:9191/help
echo
curl http://localhost:9191/ver
echo
curl http://localhost:9191/stat
echo
curl http://localhost:9191/clusters
echo
