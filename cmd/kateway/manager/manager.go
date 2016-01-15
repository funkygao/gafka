package manager

type Manager interface {
	Name() string

	Start()
	Stop()

	AuthPub(appid, pubkey, topic string) error
	AuthSub(appid, subkey, topic string) error
	LookupCluster(appid string) (cluster string, found bool)
}

var Default Manager
