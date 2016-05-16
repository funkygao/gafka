# Blockchain

Each transaction is broadcast to every node in the Bitcoin network and is then recorded in a public ledger after verification.

### Bitcoin

The Bitcoin blockchain is about 50 GB; it grew by 24 GB in 2015

- Distributed Consensus Algorithm: POW
- Replication: Full
- Communication Protocol: P2P Gossip

### BlockHeader

    type BlockHeader struct {
        Version int32       // currently it is always 4
        PrevBlock [32]byte  // sha32 hash of the the previous block
        MerkleRoot [32]byte
        Timestamp time.Time
        Bits uint32         // Difficulty target for the block
        Nonce uint32
    }
