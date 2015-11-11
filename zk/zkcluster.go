package zk

type ZkCluster struct {
	zone *ZkZone
	name string // cluster name
	path string // cluster's kafka chroot path in zk cluster
}

func (this *ZkCluster) GetTopics(cluster string) []string {
	r := make([]string, 0)
	for name, _ := range this.zone.getChildrenWithData(clusterRoot + BrokerTopicsPath) {
		r = append(r, name)
	}
	return r

}
