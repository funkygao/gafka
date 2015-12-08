package dumb

import (
	"sync"
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
	return "dumb"
}

func (this *pubStore) SyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {
	return
}

func (this *pubStore) AsyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {

	return
}
