# kafka operations

### Zookeeper

- SSD dramatically improve performance
- 5-node ZK ensemble(twitter runs 13-node ensemble)
- 1 ZK ensemble for all kafka clusters within a data center

### Kafka

#### Hardware

- 64GB mem

  4GB for kafka broker, 60GB for page cache

- RAID10 w/ 14 disks

#### OS

- vm.swappiness=0
- ext4 and flush interval 2m

#### Java

- Oracle JDK7u51+
- GC tuning

    java -Xms4g -Xmx4g -XX:PermSize=48m -XX:MaxPermSize=48m
         -XX:+UseG1GC
         -XX:MaxGCPauseMillis=20
         -XX:InitiatingHeapOccupancyPercent=35

#### kafka server.properties

- num.io.threads >= #disks
- num.network.threads






        SocketServer has 1 Acceptor and num.network.threads Processor
                     |
                RequestChannel
                     |
        num.io.threads KafkaRequestHandler forms a KafkaRequestHandlerPool
                           |
                        KafkaApis.handle

        /data3/kfk_logs/replication-offset-checkpoint

        TopicConfigManager /config/changes/config_change_13321 and delete older than 15m config_change_*
        KafkaHealthcheck   /brokers/ids/$id

        ReplicaManager

        KafkaController
            elect就是创建临时节点/controller，成功者成为controller

            ControllerChannelManager

            onControllerFailover
                成为controller
                    /admin/reassign_partitions
                    /admin/preferred_replica_election

                    PartitionStateMachine
                        /brokers/topics         TopicChangeListener
                        /admin/delete_topics    DeleteTopicsListener

                    ReplicaStateMachine
                        /brokers/ids            BrokerChangeListener



            onControllerResignation
                以前是controller，现在不是了，重新elect

            /controller_epoch 记录controller变化的全部过程，解决race condition问题 CAS

