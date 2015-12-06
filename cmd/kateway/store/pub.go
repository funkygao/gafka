package store

type PubStore interface {
	Start() error

	SyncPub(cluster string, topic, key string, data []byte) (partition int32,
		offset int64, err error)
	AsyncPub(cluster string, topic, key string, data []byte) (partition int32,
		offset int64, err error)
}

var DefaultPubStore PubStore
