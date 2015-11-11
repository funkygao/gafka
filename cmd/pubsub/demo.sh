./pubsub 
app=demo5
./pubsub app    -init $app
./pubsub inbox  -id $app -add newuser
./pubsub inbox  -id $app -list
./pubsub outbox -id $app -add orderStatus
./pubsub outbox -id $app -list
