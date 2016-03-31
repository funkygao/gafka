# Consensus in Raft

### Commands

- RequestVoteRequest/RequestVoteResponse
- AppendEntriesRequest/AppendEntriesResponse
- InstallSnapshotRequest/InstallSnapshotResponse


### Interfaces

    func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore,
        peerStore PeerStore, trans Transport) (*Raft, error)

#### FSM

    Apply(*Log) interface{}
    Snapshot() (FSMSnapshot, error)
    Restore(io.ReadCloser) error

#### StableStore

    Set(key []byte, val []byte) error
    Get(key []byte) ([]byte, error)
    SetUint64(key []byte, val uint64) error
    GetUint64(key []byte) (uint64, error)

#### StreamLayer

    net.Listener
    Dial(address string, timeout time.Duration) (net.Conn, error)

#### PeerStore

    Peers() ([]string, error)
    SetPeers([]string) error

#### FSMSnapshot

    Persist(sink SnapshotSink) error
    Release()

#### SnapshotSink

    io.WriteCloser
    ID() string
    Cancel() error

#### LogStore

    FirstIndex() (uint64, error)
    LastIndex() (uint64, error)
    GetLog(index uint64, log *Log) error
    StoreLog(log *Log) error
    StoreLogs(logs []*Log) error
    DeleteRange(min, max uint64) error

