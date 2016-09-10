package zk

import (
	"bytes"
	"fmt"

	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type zkreg struct {
	id     string
	zkzone *zk.ZkZone
	data   []byte
}

func New(zkzone *zk.ZkZone, id string, data []byte) *zkreg {
	this := &zkreg{
		id:     id,
		zkzone: zkzone,
		data:   data,
	}

	return this
}

func (this *zkreg) mypath() string {
	return fmt.Sprintf("%s/%s", Root(this.zkzone.Name()), this.id)
}

func (this *zkreg) Name() string {
	return "zookeeper"
}

func (this *zkreg) Register() error {
	err := this.zkzone.CreateEphemeralZnode(this.mypath(), this.data)
	if err == nil {
		log.Debug("registered in zk: %s", this.mypath())
	}

	return err
}

func (this *zkreg) Registered() (ok bool, err error) {
	ok, _, err = this.zkzone.Conn().Exists(this.mypath())
	return
}

func (this *zkreg) Deregister() error {
	data, _, err := this.zkzone.Conn().Get(this.mypath())
	if err != nil {
		return err
	}

	// ensure I own this znode
	if !bytes.Equal(data, this.data) {
		return fmt.Errorf("registry[%s] exp %, got %s", this.id, string(this.data), string(data))
	}

	return this.zkzone.Conn().Delete(this.mypath(), -1)
}
