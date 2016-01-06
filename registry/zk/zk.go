package zk

import (
	"errors"
	"fmt"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

const (
	KatewayIdsRoot = "/_kateway/ids"
)

type zkreg struct {
	id     string
	zkzone *zk.ZkZone
	data   []byte
}

func New(zone string, id string, data []byte) *zkreg {
	this := &zkreg{
		zkzone: zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone))),
		id:     id,
		data:   data,
	}

	return this
}

func (this *zkreg) mypath() string {
	return fmt.Sprintf("%s/%s", KatewayIdsRoot, this.id)
}

func (this *zkreg) Register() error {
	return this.zkzone.CreateEphemeralZnode(this.mypath(), this.data)
}

func (this *zkreg) Deregister() error {
	data, _, err := this.zkzone.Conn().Get(this.mypath())
	if err != nil {
		return err
	}

	// ensure I own this znode
	if string(data) != string(this.data) {
		return errors.New("a stranger intrudes:" + string(data))
	}

	return this.zkzone.Conn().Delete(this.mypath(), -1)
}
