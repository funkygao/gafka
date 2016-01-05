package zk

import (
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

func (this *zkreg) Register() error {
	return this.zkzone.CreateEphemeralZnode(
		fmt.Sprintf("%s/%s", KatewayIdsRoot, this.id), this.data)
}

func (this *zkreg) Deregister() error {
	return nil
}
