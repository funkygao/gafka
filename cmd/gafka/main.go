package main

import (
	"fmt"
	"os"

	"github.com/mitchellh/cli"
)

func main() {
	app := os.Args[0]
	c := cli.NewCLI(app, version)
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
