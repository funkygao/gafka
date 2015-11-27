# influxdb for kafka metrics storage


### Why not mysql

- index key is timestamp

    index cannot be held be memory
    乱序入库
    seek vs scan
    流式追加、区间检索

### Terms

    <measurement>[,<tag-key>=<tag-value>...] <field-key>=<field-value>[,<field2-key>=<field2-value>...] [unix-nano-timestamp]

#### Retention policy

- duration
- replication

### CLI

    > INSERT cpu,host=serverA,region=us_west value=0.64
    > SELECT * FROM cpu
    > INSERT temperature,machine=unit42,type=assembly external=25,internal=37

### Memo

- fields are not indexed

### WritePoints

    shard mapping points according to retention policy
    for shardId, points := range points {
        go writeToShard {
            for owner := range shard.Owners {
                go func() {
                    if owner.NodeID == my.meta.NodeId {
                        // local 
                        my.tsdb.writeToShard(shardId, points)
                    } else {
                        // remote 
                        my.shardWriter.writeToShard(shardId, owner.NodeId, points)
                    }
                }
            }
        }
    }


### TSM

    Queries to the storage engine will merge data from the WAL with data from the index. 


    WAL: TLV  T:1B L:4B


    each block contains up to 1000 values
    block.id = fnv64(measurement name, tagset, field name)

    0000000: 16d1 16d1 9a08 c643 7ecd b713 0000 0026  .......C~......&
             --------- ------------------- ---------
             magic     block id            block len

    0000010: 1418 4122 993f 0725 0009 1c14 1841 2299  ..A".?.%.....A".
             ===================
             block min timestamp

    0000020: 3f07 2510 3fec cccc cccc cccd c3ec 014c  ?.%.?..........L
    0000030: cccc cccc ccc0 9a08 c643 7ecd b713 0000  .........C~.....
                            ------------------- ====
                            idx block id        idx block of

    0000040: 0004 1418 4122 993f 0725 1418 4122 993f  ....A".?.%..A".?
             ==== =================== =============
             fset min timestamp       max timestamp

    0000050: 0726 0000 0001                           .&....
             ==== ---------
                  series cnt
