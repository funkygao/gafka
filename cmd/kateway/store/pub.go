package store

type PubStore interface {
	Start() error
	Stop()
	Name() string

	/* TODO
	SyncPub(cluster string, topic, key string, in io.Reader) (partition int32,
		offset int64, err error)*/
	SyncPub(cluster string, topic, key string, data []byte) (partition int32,
		offset int64, err error)
	AsyncPub(cluster string, topic, key string, data []byte) (partition int32,
		offset int64, err error)
}

var DefaultPubStore PubStore
