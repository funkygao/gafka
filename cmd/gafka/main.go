package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/funkygao/gafka/ver"
	"github.com/funkygao/gocli"
)

func main() {
	log.SetOutput(ioutil.Discard)

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
	}
	c := cli.NewCLI(app, ver.Version+"-"+ver.BuildId)
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
