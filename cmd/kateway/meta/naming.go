package meta

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
