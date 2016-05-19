#!/bin/sh
#----------------------
# play with kateway
#----------------------

# register the demo cluster 'me'
zk create -p /kafka_pubsub
gk clusters -z local -add me -p /kafka_pubsub
gk clusters -z local -c me -s -public 1 -nickname me

# register a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' 'http://localhost:9193/v1/topics/me/app1/foobar/v1?partitions=1&replicas=1'

# register a shadow topic
curl -i -XPOST -H'Appid: app14' -H'Subkey: mysubkey' http://localhost:9192/v1/guard/app1/foobar/v1/mygroup1

cd cmd/kateway/bench
make pub
make sub

# sub a topic
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foobar/v1?group=group1&reset=newest&limit=1'
curl -XGET -H'Appid: app2' -H'Subkey: mysubkey' 'http://localhost:9192/v1/msgs/app1/foo/v1?group=group1'

# pub a topic
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d 'hello world!' 'http://localhost:9191/v1/msgs/foobar/v1'
curl -XPOST -H'Appid: app1' -H'Pubkey: mypubkey' -d '@/Users/funky/gopkg/src/github.com/funkygao/fae/contrib/resources/dashboard.png' 'http://localhost:9191/v1/msgs/foobar/v1'

# raw sub
curl -XGET -H'Appid: app1' -H'Subkey: mysubkey' 'http://localhost:9192/v1/raw/msgs/app1/foobar/v1?group=xx'

curl -XPUT -H'Appid: app2' -H'Subkey: mysubkey' -d '[{"partition":0,"offset":1221300}]' http://localhost:9192/v1/offsets/app1/foobar/v1/bench_go


#----------------------
# play with meta
#----------------------
curl http://localhost:9193/v1/status
echo
curl http://localhost:9193/v1/clusters
echo


# setup cluster nickname
gk clusters -z pre -s -c psub -nickname pubsub演示
gk clusters -z pre -s -c bp_backend -nickname bp后台
gk clusters -z pre -s -c comment -nickname 评论
gk clusters -z pre -s -c activitycenter_platform -nickname 活动中心
gk clusters -z pre -s -c coupon -nickname 券
gk clusters -z pre -s -c flashtrade_web -nickname 闪购
gk clusters -z pre -s -c goods -nickname 商品
gk clusters -z pre -s -c logstash -nickname 日志
gk clusters -z pre -s -c msgcenter -nickname 消息中心
gk clusters -z pre -s -c payment -nickname 支付
gk clusters -z pre -s -c plaza_pusher -nickname 广场推送
gk clusters -z pre -s -c point -nickname 积分
gk clusters -z pre -s -c recommender -nickname 推荐
gk clusters -z pre -s -c search -nickname 搜索
gk clusters -z pre -s -c ticket -nickname 电影票
gk clusters -z pre -s -c trade -nickname 交易
gk clusters -z pre -s -c ucenter -nickname 用户中心
gk clusters -z pre -s -c wifi_portal -nickname 广场wifi
