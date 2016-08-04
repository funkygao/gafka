package main

import (
	"fmt"
	"os"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/actor/scheduler"
)

func init() {

}

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "-v" || arg == "-version" {
			fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
			return
		}
	}

}
