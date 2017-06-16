package zk

import (
	"fmt"
)

type EsCluster struct {
	Name   string
	zkzone *ZkZone
}

func (ec *EsCluster) AddNode(hostPort string) error {
	path := fmt.Sprintf("%s/%s/node/%s", esRoot, ec.Name, hostPort)
	if err := ec.zkzone.ensureParentDirExists(path); err != nil {
		return err
	}

	return ec.zkzone.CreatePermenantZnode(path, nil)
}

func (ec *EsCluster) Nodes() []string {
	path := fmt.Sprintf("%s/%s/node", esRoot, ec.Name)
	return ec.zkzone.children(path)
}
