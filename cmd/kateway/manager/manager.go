package manager

// Manager is the interface that integrates with pubsub manager UI.
type Manager interface {
	// Name of the manager implementation.
	Name() string

	Start()
	Stop()

	AuthPub(appid, pubkey, topic string) error
	AuthSub(appid, subkey, topic string) error
	LookupCluster(appid string) (cluster string, found bool)
}

var Default Manager
