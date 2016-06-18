package dummy

import (
	"net/http"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/mpool"
)

type dummyStore struct {
}

func New() *dummyStore {
	return &dummyStore{}
}

func (this *dummyStore) Name() string {
	return "dummy"
}

func (this *dummyStore) KafkaTopic(appid string, topic string, ver string) (r string) {
	b := mpool.BytesBufferGet()
	b.Reset()
	b.WriteString(appid)
	b.WriteString(".")
	b.WriteString(topic)
	b.WriteString(".")
	b.WriteString(ver)
	r = b.String()
	mpool.BytesBufferPut(b)
	return
}

func (this *dummyStore) ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group string) (r string) {
	r = this.KafkaTopic(hisAppid, topic, ver)
	return r + "." + myAppid + "." + group + "." + shadow
}

func (this *dummyStore) WebHooks() ([]manager.WebHook, error) {
	return nil, nil
}

func (this *dummyStore) DeadPartitions() map[string]map[int32]struct{} {
	return nil
}

func (this *dummyStore) AuthAdmin(appid, pubkey string) bool {
	return true
}

func (this *dummyStore) ValidateTopicName(topic string) bool {
	return true
}

func (this *dummyStore) ValidateGroupName(header http.Header, group string) bool {
	return true
}

func (this *dummyStore) OwnTopic(appid, pubkey, topic string) error {
	return nil
}

func (*dummyStore) AllowSubWithUnregisteredGroup(yes bool) {

}

func (this *dummyStore) AuthSub(appid, subkey, hisAppid, hisTopic, group string) error {
	return nil
}

func (this *dummyStore) LookupCluster(appid string) (string, bool) {
	return "me", true
}

func (this *dummyStore) IsShadowedTopic(hisAppid, topic, ver, myAppid, group string) bool {
	return true
}

func (this *dummyStore) Dump() map[string]interface{} {
	return nil
}

func (this *dummyStore) Start() error {
	return nil
}

func (this *dummyStore) Stop() {}
