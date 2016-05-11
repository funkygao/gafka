package dummy

import (
	"net/http"

	"github.com/funkygao/gafka/cmd/kateway/manager"
)

type dummyStore struct {
}

func New() *dummyStore {
	return &dummyStore{}
}

func (this *dummyStore) Name() string {
	return "dummy"
}

func (this *dummyStore) WebHooks() ([]manager.WebHook, error) {
	return nil, nil
}

func (this *dummyStore) Refreshed() <-chan struct{} {
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
