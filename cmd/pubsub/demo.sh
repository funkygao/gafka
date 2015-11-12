# zk create /_pubsub
# zk create /_pubsub/bind

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
