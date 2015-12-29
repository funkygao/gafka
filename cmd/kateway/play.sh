#!/bin/sh
#----------------------
# play with kateway
#----------------------

# register the demo cluster 'me'
gk clusters -z local -add me -p /kafka_pubsub

# register a topic
curl -XPOST -H'Appid: myapp' -H'Pubkey: mypubkey' 'http://localhost:9193/topics/me/foobar/v1?partitions=1&replicas=1'

# sub a topic
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/topics/app1/foobar/v1/group1?reset=newest&limit=1'

# raw sub
curl -XGET http://localhost:9192/raw/topics/app1/foobar/v1

# pub a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/topics/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d '@/Users/funky/gopkg/src/github.com/funkygao/fae/contrib/resources/dashboard.png' 'http://localhost:9191/topics/foobar/v1'

# raw pub
curl -XGET -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9191/raw/topics/foobar/v1'


#----------------------
# play with meta
#----------------------
curl http://localhost:9193/help
echo
curl http://localhost:9193/ver
echo
curl http://localhost:9193/stat
echo
curl http://localhost:9193/clusters
echo
