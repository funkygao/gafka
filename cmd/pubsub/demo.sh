# How to setup a new demo env
#============================
# 1. rm /tmp/zookeeper
# 2. restart zk
#
# 1. zk create /kafka_pubsub
# 2. rm -rf /tmp/kafka*
# 3. ./bin/kafka-topics.sh --zookeeper localhost:2181/kafka_pubsub --create --topic _bindings --replication-factor 1 --partitions 1
#
# 1. exchanged demo.sh

./pubsub 

# app1
app=app1
./pubsub app    -init $app
./pubsub inbox  -id $app -add in1
./pubsub inbox  -id $app -list
./pubsub outbox -id $app -add out1
./pubsub outbox -id $app -list

# app2
app=app2
./pubsub app    -init $app
./pubsub inbox  -id $app -add in2
./pubsub inbox  -id $app -list
./pubsub outbox -id $app -add out2
./pubsub outbox -id $app -list

# app3
app=app3
./pubsub app    -init $app
./pubsub inbox  -id $app -add in3
./pubsub inbox  -id $app -list
./pubsub outbox -id $app -add out3
./pubsub outbox -id $app -list

# bind (out1, out2) -> in3
./pubsub bind -id app3 -add -from app1:out1 -to in3
./pubsub bind -id app3 -add -from app2:out2 -to in3

# ./pubsub pub -id app1 -topic out1
# ./pubsub sub -id app3
