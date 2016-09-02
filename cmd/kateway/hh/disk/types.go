package disk

import (
	"path/filepath"
)

type block struct {
	key   []byte
	value []byte
}

func (b *block) size() int64 {
	return int64(len(b.key) + len(b.value) + 8)
}

func (b *block) keyLen() uint32 {
	return uint32(len(b.key))
}

func (b *block) valueLen() uint32 {
	return uint32(len(b.value))
}

type clusterTopic struct {
	cluster, topic string
}

func (ct clusterTopic) ClusterDir(base string) string {
	return filepath.Join(base, ct.cluster)
}

func (ct clusterTopic) TopicDir(base string) string {
	return filepath.Join(base, ct.cluster, ct.topic)
}
