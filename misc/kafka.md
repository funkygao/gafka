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
