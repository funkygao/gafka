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
	"github.com/funkygao/gocli"
	"github.com/funkygao/log4go"
	"github.com/funkygao/zkclient"
)

func listChildren(zone, rootPath string) {
	fmt.Println(rootPath)

	zc := zkclient.New(ctx.ZoneZkAddrs(zone))
	if err := zc.Connect(); err != nil {
		panic(err)
	}
	children, _ := zc.Children(rootPath)
	for _, c := range children {
		fmt.Println("/" + c)
	}
}

func main() {
	ctx.LoadFromHome()
	log.SetOutput(ioutil.Discard)
	log4go.AddFilter("stdout", log4go.INFO, log4go.NewConsoleLogWriter())

	var zone = ctx.DefaultZone()
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
				for i, arg := range args {
					if arg == "-z" && len(args) > i {
						zone = args[i+1]
					}
				}

				lastArg := args[len(args)-2]
				switch lastArg {
				case "-z": // zone
					for _, z := range ctx.SortedZones() {
						fmt.Println(z)
					}

				default:
					// autocomplete the root children
					listChildren(zone, "/")
				}

				return
			}

			for name, _ := range commands {
				fmt.Println(name)
			}

			return
		}
	}
	c := cli.NewCLI(app, gafka.Version+"-"+gafka.BuildId)
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
		buf.WriteString(fmt.Sprintf("A CLI tool for Zookeeper\n\n"))
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
