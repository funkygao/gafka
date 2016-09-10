package dummy

type pubStore struct {
}

func NewPubStore(debug bool) *pubStore {
	return &pubStore{}
}

func (this *pubStore) Start() (err error) {
	return
}

func (this *pubStore) Stop() {}

func (this *pubStore) Name() string {
	return "dummy"
}

func (this *pubStore) IsSystemError(error) bool {
	return false
}

func (this *pubStore) SyncAllPub(cluster string, topic string, key,
	msg []byte) (partition int32, offset int64, err error) {
	return
}

func (this *pubStore) SyncPub(cluster string, topic string, key,
	msg []byte) (partition int32, offset int64, err error) {
	return
}

func (this *pubStore) AsyncPub(cluster string, topic string, key,
	msg []byte) (partition int32, offset int64, err error) {

	return
}
