# ZK in kafka

### consumer group

    /consumers/$group/ids/$group_$hostname-$timestamp-$uuid -> 
        {"version":1, "subscription": {$topic: $count}, "pattern": $pattern,"timestamp": $timestamp}

    // 如果rebalance时发现没用broker，那么就watch该znode，发现有broker出现时rebalance
    // 如果已经有broker了，就不watch该znode了
    subscribeChildChanges("/brokers/ids", loadBalancerListener) 

    // 监视群组里成员的变化
    subscribeChildChanges("/consumers/$group/ids", loadBalancerListener)

    // 监视订阅的topic的partition的变化，触发rebalance
    subscribeDataChanges("/brokers/topics/$topic", topicPartitionChangeListener)

    // zk session失效后，重新注册consumer并rebalance
    subscribeStateChanges(sessionExpirationListener)

#### rebalance

    取得我订阅的topic {/consumers/$mygrp/ids/$myid: subscriptions}
    取得我组内所有成员 /consumers/$mygrp/ids/*，并取得每个成员的subscriptions
    取得我订阅的每个topic分区信息 {/brokers/topics/$topic: partitions}

    关闭我的fetchers，commit offset
    释放owner权限 delete /consumers/$mygrp/owners/$topic/$partition

    # 决策算法
    现在已经知道某个topic的如下信息：
        本group里有多少consumer，该topic有多少partition
    把它们排序后，进行配对分配，从而知道我分得哪些partition的消费权

    对我消费的每个partition，取得offset /consumers/$mygrp/offsets/$topic/$partition

    发布owner权，占领partition  # 可能失败
    启动fetchers

