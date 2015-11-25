package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gocli"
	"github.com/funkygao/log4go"
)

func main() {
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
			for name, _ := range commands {
				fmt.Println(name)
			}
			return
		}
	}
	c := cli.NewCLI(app, gafka.Version+"-"+gafka.BuildId)
	c.Args = os.Args[1:]
	c.Commands = commands
	c.HelpFunc = cli.BasicHelpFunc(app)

	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}

	os.Exit(exitCode)
}

func setupLogging() {
	log.SetOutput(ioutil.Discard)

	level := log4go.DEBUG // TODO

	for _, filter := range log4go.Global {
		filter.Level = level
	}

	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())
}
