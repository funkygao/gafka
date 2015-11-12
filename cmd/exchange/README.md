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

    
