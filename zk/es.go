package zk

type EsCluster struct {
	Name   string
	zkzone *ZkZone
}

func (ec *EsCluster) AddNode(hostPort string) error {
	return nil
}

func (ec *EsCluster) Nodes() []string {
	return nil
}
