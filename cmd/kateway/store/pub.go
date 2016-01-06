package store

// A PubStore is a generic store that can Pub sync/async.
type PubStore interface {
	// Name returns the name of the underlying store.
	Name() string

	Start() error
	Stop()

	// SyncPub pub a keyed message to a topic of a cluster synchronously.
	SyncPub(cluster, topic string, key, msg []byte) error

	// AsyncPub pub a keyed message to a topic of a cluster asynchronously.
	AsyncPub(cluster, topic string, key, msg []byte) error
}

var DefaultPubStore PubStore
