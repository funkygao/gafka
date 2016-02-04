package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

var (
	options struct {
		zone     string
		cluster  string
		mount    string
		boot     bool
		logLevel string
		version  bool
	}
)

func parseFlags() {
	flag.StringVar(&options.zone, "z", "", "kafka zone name")
	flag.StringVar(&options.cluster, "c", "", "kafka cluster name")
	flag.StringVar(&options.mount, "mount", "", "mount point")
	flag.BoolVar(&options.boot, "b", false, "boot guide")
	flag.StringVar(&options.logLevel, "l", "info", "log level")
	flag.BoolVar(&options.version, "version", false, "show version and exit")

	flag.Parse()
}

func validateFlags() {
	if options.zone == "" {
		fmt.Fprintf(os.Stderr, "-z required\n")
		os.Exit(1)
	}

	if options.cluster == "" {
		fmt.Fprintf(os.Stderr, "-c required\n")
		os.Exit(1)
	}

	if options.mount == "" {
		fmt.Fprintf(os.Stderr, "-mount required\n")
		os.Exit(1)
	}

	if !strings.HasPrefix(options.mount, "/") {
		fmt.Fprintf(os.Stderr, "mount point must start with /\n")
		os.Exit(1)
	}

}
