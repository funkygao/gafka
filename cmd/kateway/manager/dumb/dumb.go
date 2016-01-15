package dumb

type dumbStore struct {
}

func New() *dumbStore {
	return &dumbStore{}
}

func (this *dumbStore) Name() string {
	return "dumb"
}

func (this *dumbStore) AuthPub(appid, pubkey, topic string) error {
	return nil
}

func (this *dumbStore) AuthSub(appid, subkey, topic string) error {
	return nil
}

func (this *dumbStore) LookupCluster(appid string) (string, bool) {
	return "me", true
}

func (this *dumbStore) Start() {}

func (this *dumbStore) Stop() {}
