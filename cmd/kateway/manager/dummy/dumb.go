package dummy

type dummyStore struct {
}

func New() *dummyStore {
	return &dummyStore{}
}

func (this *dummyStore) Name() string {
	return "dummy"
}

func (this *dummyStore) AuthPub(appid, pubkey, topic string) error {
	return nil
}

func (this *dummyStore) AuthSub(appid, subkey, topic string) error {
	return nil
}

func (this *dummyStore) LookupCluster(appid string) (string, bool) {
	return "me", true
}

func (this *dummyStore) Start() {}

func (this *dummyStore) Stop() {}
