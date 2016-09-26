package zk

import (
	"bytes"
	"fmt"

	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type zkreg struct {
	zkzone *zk.ZkZone
}

func New(zkzone *zk.ZkZone) *zkreg {
	this := &zkreg{
		zkzone: zkzone,
	}

	return this
}

func (this *zkreg) mypath(id string) string {
	return fmt.Sprintf("%s/%s/%s", zk.KatewayIdsRoot, this.zkzone.Name(), id)
}

func (this *zkreg) Name() string {
	return "zookeeper"
}

func (this *zkreg) Register(id string, data []byte) error {
	err := this.zkzone.CreateEphemeralZnode(this.mypath(id), data)
	if err == nil {
		log.Debug("registered in zk: %s", this.mypath(id))
		return nil
	}

	return fmt.Errorf("%s %v", this.mypath(id), err)
}

func (this *zkreg) Registered(id string) (ok bool, err error) {
	ok, _, err = this.zkzone.Conn().Exists(this.mypath(id))
	return
}

func (this *zkreg) Deregister(id string, oldData []byte) error {
	data, _, err := this.zkzone.Conn().Get(this.mypath(id))
	if err != nil {
		return fmt.Errorf("%s %v", this.mypath(id), err)
	}

	// ensure I own this znode
	if !bytes.Equal(data, oldData) {
		return fmt.Errorf("registry[%s] exp %, got %s", id, string(oldData), string(data))
	}

	return this.zkzone.Conn().Delete(this.mypath(id), -1)
}

func (this *zkreg) WatchInstances() ([]string, <-chan zklib.Event, error) {
	path := fmt.Sprintf("%s/%s", zk.KatewayIdsRoot, this.zkzone.Name())
	ids, _, ch, err := this.zkzone.Conn().ChildrenW(path)
	if err != nil {
		return nil, nil, fmt.Errorf("%s %v", path, err)
	}

	instancePaths := make([]string, 0, len(ids))
	for _, id := range ids {
		instancePaths = append(instancePaths, fmt.Sprintf("%s/%s", path, id))
	}

	return instancePaths, ch, nil
}
