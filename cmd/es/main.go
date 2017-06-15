// es is the ElasticSearch console.
package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/log4go"
)

func main() {
	ctx.LoadFromHome()
	setupLogging()

	app := os.Args[0]
	args := os.Args[1:]
	for _, arg := range args {
		if arg == "-v" || arg == "--version" {
			newArgs := make([]string, len(args)+1)
			newArgs[0] = "version"
			copy(newArgs[1:], args)
			args = newArgs
			break
		}

		if arg == "--generate-bash-completion" {
			if len(args) > 1 {
				// contextual auto complete
				lastArg := args[len(args)-2]
				switch lastArg {
				case "-z": // zone
					for _, zone := range ctx.SortedZones() {
						fmt.Println(zone)
					}
					return

				case "-c": // cluster
					zone := ctx.ZkDefaultZone()
					for i := 0; i < len(args)-1; i++ {
						if args[i] == "-z" {
							// happy with panic
							zone = args[i+1]
						}
					}
					zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
					zkzone.ForSortedEsClusters(func(ec *zk.EsCluster) {
						fmt.Println(ec.Name)
					})
					return
				}
			}

			for name := range commands {
				fmt.Println(name)
			}

			return
		}
	}

	c := cli.NewCLI(app, fmt.Sprintf("es %s (%s)", gafka.Version, gafka.BuildId))
	c.Args = os.Args[1:]
	c.Commands = commands
	c.HelpFunc = func(m map[string]cli.CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("ElasticSearch Console\n\n"))
		buf.WriteString(cli.BasicHelpFunc(app)(m))
		return buf.String()
	}

	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	} else if c.IsVersion() {
		os.Exit(0)
	}

	os.Exit(exitCode)
}

func setupLogging() {
	log.SetOutput(ioutil.Discard)

	level := log4go.ToLogLevel(ctx.LogLevel(), log4go.DEBUG)
	log4go.SetLevel(level)
	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())
}
