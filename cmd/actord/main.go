package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/actord/bootstrap"
	"github.com/funkygao/gafka/cmd/kateway/gateway"
	log "github.com/funkygao/log4go"
)

func init() {
	gateway.EnsureServerUlimit()
	debug.SetGCPercent(800) // same as env GOGC
}

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "-v" || arg == "-version" {
			fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
			return
		}
	}

	t0 := time.Now()
	bootstrap.Main()
	log.Info("actor[%s@%s] %s, bye!", gafka.BuildId, gafka.BuiltAt, time.Since(t0))
	log.Close()
}
