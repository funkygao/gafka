package disk

import (
	"path/filepath"
)

type clusterTopic struct {
	cluster, topic string
}

func (ct clusterTopic) ClusterDir(base string) string {
	return filepath.Join(base, ct.cluster)
}

func (ct clusterTopic) TopicDir(base string) string {
	return filepath.Join(base, ct.cluster, ct.topic)
}
