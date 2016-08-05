package bootstrap

import (
	"flag"

	"github.com/funkygao/gafka/cmd/actor/controller"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

func init() {
	flag.StringVar(&Options.Zone, "z", "", "zone")
	flag.Parse()

	if Options.Zone == "" {
		panic("empty zone not allowed")
	}

	ctx.LoadFromHome()
}

// Main is the bootstrap main entry point, which will run for ever.
func Main() {
	zkzone := zk.NewZkZone(zk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))
	c := controller.New(zkzone)
	if err := c.ServeForever(); err != nil {
		panic(err)
	}

}
