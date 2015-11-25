# exchange

### zk

    On startup, each exchange will register an ephemeral and sequence znode in /_pubsub/exchange.
    Each exchange will subscribeChildChanges("/_pubsub/exchange") and will trigger rebalance on child node change.

### rebalance

    get and sort all bindings    /_pubsub/bind
    get and sort all exchanges   /_pubsub/exchange

    stop all consuming threads and wait till producing threads flushed all inflight msg
    reassign exchange ownership of bindings
    start routing

### RabbitMQ

#### Terms

- Connection
- Channel
  - queue, exchange, bind(queue, exchange)
  - publish
  - exchange
    - fanout
    - direct
    - topic
    - headers


                    binding
    P --> Exchange ---------> Queue --> C
                    routing
                    key


    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) 
    channel = connection.channel()  
    queue = channel.queue_declare(queue='hello')  
    channel.exchange_declare(exchange='logs', type='fanout')
    channel.queue_bind(exchange='logs', queue=queue.method.queue)
    channel.basic_publish(exchange='logs', routing_key='hello', body='Hello world')
    connection.close()

    # consumer
    channel.basic_consume(callback,  queue='hello', no_ack=True)  
    channel.start_consuming()
    def callback(ch, method, properties, body):
        pass
