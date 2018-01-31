package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/log4go"
)

func main() {
	ctx.LoadFromHome()
	setupLogging()
	//audit()

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
					// not 'gk redis -c'
					zone := ctx.ZkDefaultZone()
					for i := 0; i < len(args)-1; i++ {
						if args[i] == "-z" {
							zone = args[i+1]
						}
					}
					zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))

					gkRedis := false
					for _, arg := range args {
						if arg == "redis" {
							gkRedis = true // gk redis -c
							break
						}
					}

					if gkRedis {
						for _, c := range zkzone.AllRedisClusters() {
							fmt.Println(c.Name)
						}
						return
					}

					// not 'gk redis -c'
					zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
						fmt.Println(zkcluster.Name())
					})
					return

				case "-t": // topic
					zone := ctx.ZkDefaultZone()
					var cluster string
					for i := 0; i < len(args)-1; i++ {
						if args[i] == "-z" {
							zone = args[i+1]
						}

						if args[i] == "-c" {
							cluster = args[i+1]
						}
					}
					if cluster != "" {
						zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
						zkcluster := zkzone.NewCluster(cluster)
						topics, _ := zkcluster.Topics()
						for _, t := range topics {
							fmt.Println(t)
						}
					}
					return
				}

			}

			for name := range commands {
				fmt.Println(name)
			}
			for _, cmd := range ctx.Aliases() {
				fmt.Println(cmd)
			}
			return
		}
	}

	c := cli.NewCLI(app, fmt.Sprintf("gk %s (%s@%s)", gafka.Version, gafka.BuildId, gafka.BuiltAt))
	c.Args = os.Args[1:]
	if len(os.Args) > 1 {
		// command given, convert alias
		if alias, present := ctx.Alias(os.Args[1]); present {
			var cargs []string
			cargs = append(cargs, strings.Split(alias, " ")...)
			if len(os.Args) > 2 {
				cargs = append(cargs, os.Args[2:]...)
			}
			c.Args = cargs
		}
	}
	c.Commands = commands
	c.HelpFunc = func(m map[string]cli.CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("Unified multi-datacenter multi-cluster kafka swiss-knife management console\n\n"))
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
