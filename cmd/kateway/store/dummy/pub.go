package dummy

import (
	"sync"
	"time"
)

type pubStore struct {
}

func NewPubStore(wg *sync.WaitGroup, debug bool) *pubStore {
	return &pubStore{}
}

func (this *pubStore) Start() (err error) {
	return
}

func (this *pubStore) Stop() {}

func (this *pubStore) Name() string {
	return "dummy"
}

func (this *pubStore) MarkPartitionsDead(topic string, partitionIds map[int32]struct{}) {

}

func (*pubStore) AddJob(cluster, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	return "", nil
}

func (*pubStore) DeleteJob(cluster, jobId string) error {
	return nil
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
