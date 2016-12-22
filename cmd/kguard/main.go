package main

import (
	"fmt"
	"os"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	_ "github.com/funkygao/gafka/cmd/kguard/sos"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/actord"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/anomaly"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/external"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/haproxy"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/influxdb"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/influxquery"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kafka"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/kateway"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/redis"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/zk"
	_ "github.com/funkygao/gafka/cmd/kguard/watchers/zone"
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
