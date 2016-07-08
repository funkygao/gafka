package zk

import (
	"errors"
	"fmt"

	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type zkreg struct {
	id     string
	zkzone *zk.ZkZone
	data   []byte

	shutdownCh chan struct{}
}

func New(zkzone *zk.ZkZone, id string, data []byte) *zkreg {
	this := &zkreg{
		id:         id,
		zkzone:     zkzone,
		data:       data,
		shutdownCh: make(chan struct{}),
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

	go this.keepalive()
	return err
}

func (this *zkreg) keepalive() {
	for {
		select {
		case <-this.shutdownCh:
			return

		case evt := <-this.zkzone.SessionEvents():
			// after zk conn lost, zklib will automatically reconnect
			// but the ephemeral znodes have to be recreated by ourselves
			if evt.State == zklib.StateHasSession {
				this.zkzone.CreateEphemeralZnode(this.mypath(), this.data)

				log.Trace("registered in zk: %s", this.mypath())
			}
		}
	}
}

func (this *zkreg) Deregister() error {
	close(this.shutdownCh)

	data, _, err := this.zkzone.Conn().Get(this.mypath())
	if err != nil {
		return err
	}

	// ensure I own this znode
	if string(data) != string(this.data) {
		return errors.New("a stranger intrudes:" + string(data))
	}

	err = this.zkzone.Conn().Delete(this.mypath(), -1)
	return err
}
