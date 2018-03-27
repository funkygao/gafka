// jp is the Java file static parser.
package main

import (
	"bytes"
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
			for name := range commands {
				fmt.Println(name)
			}

			return
		}
	}

	c := cli.NewCLI(app, fmt.Sprintf("jp %s (%s)", gafka.Version, gafka.BuildId))
	c.Args = os.Args[1:]
	c.Commands = commands
	c.HelpFunc = func(m map[string]cli.CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("Java static Parser\n\n"))
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

	level := log4go.ToLogLevel("info", log4go.DEBUG)
	log4go.SetLevel(level)
	log4go.AddFilter("stdout", level, log4go.NewConsoleLogWriter())
}
