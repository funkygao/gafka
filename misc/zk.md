# ZK in kafka

### consumer group

    subscribeChildChanges("/brokers/ids", loadBalancerListener)
    subscribeChildChanges("/consumers/$group/ids", loadBalancerListener)
    subscribeDataChanges("/brokers/topics/$topic", topicPartitionChangeListener)
    subscribeStateChanges(sessionExpirationListener)
