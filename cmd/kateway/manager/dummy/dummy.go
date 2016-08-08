package dummy

import (
	"net/http"
	"strings"
	"sync"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/mpool"
)

type dummyStore struct {
	cluster string

	dryrunLock   sync.RWMutex
	dryrunTopics map[string]map[string]map[string]struct{}
}

func New(cluster string) *dummyStore {
	return &dummyStore{
		cluster:      cluster,
		dryrunTopics: make(map[string]map[string]map[string]struct{}),
	}
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

func (this *dummyStore) TopicAppid(kafkaTopic string) string {
	firstDot := strings.IndexByte(kafkaTopic, '.')
	if firstDot == -1 || firstDot > len(kafkaTopic) {
		return ""
	}
	return kafkaTopic[:firstDot]
}

func (this *dummyStore) IsDryrunTopic(appid, topic, ver string) bool {
	this.dryrunLock.RLock()
	_, present := this.dryrunTopics[appid][topic][ver]
	this.dryrunLock.RUnlock()

	return present
}

func (this *dummyStore) MarkTopicDryrun(appid, topic, ver string) {
	this.dryrunLock.Lock()
	defer this.dryrunLock.Unlock()

	if _, present := this.dryrunTopics[appid]; !present {
		this.dryrunTopics[appid] = make(map[string]map[string]struct{})
	}
	if _, present := this.dryrunTopics[appid][topic]; !present {
		this.dryrunTopics[appid][topic] = make(map[string]struct{})
	}
	this.dryrunTopics[appid][topic][ver] = struct{}{}
}

func (this *dummyStore) ClearDryrunTopics() {
	this.dryrunLock.Lock()
	this.dryrunTopics = make(map[string]map[string]map[string]struct{})
	this.dryrunLock.Unlock()
}

func (this *dummyStore) TopicSchema(appid, topic, ver string) (string, error) {
	return `
{
   "type" : "record",
   "namespace" : "dummy",
   "name" : "Sample",
   "fields" : [
      { "name" : "Name" , "type" : "string" },
      { "name" : "Age" , "type" : "int" }
   ]
}
	`, nil
}

func (this *dummyStore) ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group string) (r string) {
	r = this.KafkaTopic(hisAppid, topic, ver)
	return r + "." + myAppid + "." + group + "." + shadow
}

func (this *dummyStore) DeadPartitions() map[string]map[int32]struct{} {
	return nil
}

func (this *dummyStore) ForceRefresh() {

}

func (this *dummyStore) AuthAdmin(appid, pubkey string) bool {
	return true
}

func (this *dummyStore) ValidateTopicName(topic string) bool {
	return true
}

func (this *dummyStore) ValidateGroupName(header http.Header, group string) bool {
	if group == "invalid" {
		return false
	}

	return true
}

func (this *dummyStore) OwnTopic(appid, pubkey, topic string) error {
	if topic == "invalid" {
		return manager.ErrAuthorizationFail
	}

	return nil
}

func (*dummyStore) AllowSubWithUnregisteredGroup(yes bool) {

}

func (this *dummyStore) AuthSub(appid, subkey, hisAppid, hisTopic, group string) error {
	if group == "invalid" {
		return manager.ErrInvalidGroup
	}

	return nil
}

func (this *dummyStore) LookupCluster(appid string) (string, bool) {
	if appid == "invalid" {
		return "", false
	}

	return this.cluster, true
}

func (this *dummyStore) IsShadowedTopic(hisAppid, topic, ver, myAppid, group string) bool {
	return true
}

func (this *dummyStore) Dump() map[string]interface{} {
	r := make(map[string]interface{})
	r["dryrun"] = this.dryrunTopics
	return r
}

func (this *dummyStore) Start() error {
	return nil
}

func (this *dummyStore) Stop() {}
