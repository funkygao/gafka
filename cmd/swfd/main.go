package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/swfd/swf"
)

func init() {
	swf.ParseFlags()

	if swf.Options.ShowVersion {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if gafka.BuildId == "" {
		fmt.Fprintf(os.Stderr, "empty BuildId, please rebuild with build.sh\n")
	}

	debug.SetGCPercent(800)
}

func main() {
	swf.ValidateFlags()
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	fmt.Fprintln(os.Stderr, strings.TrimSpace(logo))

	swf.New().ServeForever()
}
