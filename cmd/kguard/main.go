package main

import (
	"fmt"
	"os"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/external"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/influx"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kafka"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kateway"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/zk"
)

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "-v" || arg == "-version" {
			fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
			return
		}
	}

	var m monitor.Monitor
	m.Init()
	m.ServeForever()
}
