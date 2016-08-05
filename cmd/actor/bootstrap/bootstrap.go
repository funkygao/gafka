package bootstrap

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/actor/controller"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

func init() {
	flag.StringVar(&Options.Zone, "z", "", "zone")
	flag.BoolVar(&Options.ShowVersion, "v", false, "show version and exit")
	flag.Parse()

	if Options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if Options.Zone == "" {
		panic("empty zone not allowed")
	}

	ctx.LoadFromHome()
}

// Main is the bootstrap main entry point, which will run for ever.
func Main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	zkzone := zk.NewZkZone(zk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))
	c := controller.New(zkzone)
	if err := c.ServeForever(); err != nil {
		panic(err)
	}

}
