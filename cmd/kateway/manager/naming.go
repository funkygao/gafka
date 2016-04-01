package manager

import (
	"github.com/funkygao/gafka/mpool"
)

func KafkaTopic(appid string, topic string, ver string) (r string) {
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

func ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group string) (r string) {
	r = KafkaTopic(hisAppid, topic, ver)
	return r + "." + myAppid + "." + group + "." + shadow
}
