package command

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type argsRule struct {
	cmd           cli.Command
	ui            cli.Ui
	requires      []string
	adminRequires map[string]struct{} // need admin rights: prompt password
	conditions    map[string][]string
}

func validateArgs(cmd cli.Command, ui cli.Ui) *argsRule {
	return &argsRule{
		cmd:           cmd,
		ui:            ui,
		requires:      make([]string, 0),
		adminRequires: make(map[string]struct{}),
		conditions:    make(map[string][]string),
	}
}

func (this *argsRule) require(option ...string) *argsRule {
	this.requires = append(this.requires, option...)
	return this
}

func (this *argsRule) on(whenOption string, requiredOption ...string) *argsRule {
	if _, present := this.conditions[whenOption]; !present {
		this.conditions[whenOption] = make([]string, 0)
	}
	this.conditions[whenOption] = append(this.conditions[whenOption],
		requiredOption...)
	return this
}

func (this *argsRule) requireAdminRights(option ...string) *argsRule {
	for _, opt := range option {
		this.adminRequires[opt] = struct{}{}
	}
	return this
}

func (this *argsRule) invalid(args []string) bool {
	argSet := make(map[string]struct{}, len(args))
	for _, arg := range args {
		argSet[arg] = struct{}{}
	}

	// required
	for _, req := range this.requires {
		if _, present := argSet[req]; !present {
			this.ui.Error(color.Red("%s required", req))
			this.ui.Output(this.cmd.Help())
			return true
		}
	}

	// conditions
	for when, requires := range this.conditions {
		if _, present := argSet[when]; present {
			for _, req := range requires {
				if _, found := argSet[req]; !found {
					this.ui.Error(color.Red("%s required when %s present",
						req, when))
					this.ui.Output(this.cmd.Help())
					return true
				}
			}
		}
	}

	// admin required
	adminAuthRequired := false
	for _, arg := range args {
		if _, present := this.adminRequires[arg]; present {
			adminAuthRequired = true
			break
		}
	}
	if adminAuthRequired {
		if pass := os.Getenv("GK_PASS"); Authenticator("", pass) {
			return false
		}

		pass, err := this.ui.AskSecret("password for admin(or GK_PASS): ")
		this.ui.Output("")
		if err != nil {
			this.ui.Error(err.Error())
			return true
		}
		if !Authenticator("", pass) {
			this.ui.Error("invalid admin password, bye!")
			return true
		}
	}

	return false
}

func patternMatched(s, pattern string) bool {
	if pattern != "" {
		if pattern[0] == '~' {
			// NOT
			return !strings.Contains(s, pattern[1:])
		}

		if pattern[0] == '=' {
			return s == pattern[1:]
		}

		if !strings.Contains(s, pattern) {
			return false
		}
	}

	return true
}

func refreshScreen() {
	c := exec.Command("clear")
	c.Stdout = os.Stdout
	c.Run()
}

func ensureZoneValid(zone string) {
	ctx.ZoneZkAddrs(zone) // will panic if zone not found
}

func forSortedZones(fn func(zkzone *zk.ZkZone)) {
	for _, zone := range ctx.SortedZones() {
		if strings.HasPrefix(zone, "z_") {
			// zk only
			continue
		}

		zkAddrs := ctx.ZoneZkAddrs(zone)
		if strings.TrimSpace(zkAddrs) == "" {
			continue
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, zkAddrs))
		fn(zkzone)
	}
}

func forAllSortedZones(fn func(zkzone *zk.ZkZone)) {
	for _, zone := range ctx.SortedZones() {
		zkAddrs := ctx.ZoneZkAddrs(zone)
		if strings.TrimSpace(zkAddrs) == "" {
			continue
		}

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, zkAddrs))
		fn(zkzone)
	}
}

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

type sortedStrMap struct {
	keys []string
	vals []interface{}
}

// TODO map[string]interface{}
func sortStrMap(m map[string]int) sortedStrMap {
	sortedKeys := make([]string, 0, len(m))
	for key, _ := range m {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	r := sortedStrMap{
		keys: sortedKeys,
		vals: make([]interface{}, len(m)),
	}
	for idx, key := range sortedKeys {
		r.vals[idx] = m[key]
	}

	return r
}

func printSwallowedErrors(ui cli.Ui, zkzone *zk.ZkZone) {
	errs := zkzone.Errors()
	if len(errs) == 0 {
		return
	}

	for _, e := range errs {
		ui.Error(color.Red("%v", e))
	}
}

func shortIp(host string) string {
	parts := strings.SplitN(host, ".", 4)
	return strings.Join(parts[2:], ".")
}

func toLogLevel(levelStr string) log.Level {
	level := log.TRACE
	switch levelStr {
	case "info":
		level = log.INFO

	case "warn":
		level = log.WARNING

	case "error":
		level = log.ERROR

	case "debug":
		level = log.DEBUG

	case "trace":
		level = log.TRACE

	case "alarm":
		level = log.ALARM
	}

	return level
}

func setupLogging(logFile, level, crashLogFile string) {
	logLevel := toLogLevel(level)

	for _, filter := range log.Global {
		filter.Level = logLevel
	}

	log.LogBufferLength = 32 // default 32, chan cap

	if logFile == "stdout" {
		log.AddFilter("stdout", logLevel, log.NewConsoleLogWriter())
	} else {
		log.DeleteFilter("stdout")

		filer := log.NewFileLogWriter(logFile, true, false, 0)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		filer.SetRotateSize(0)
		filer.SetRotateLines(0)
		filer.SetRotateDaily(true)
		log.AddFilter("file", logLevel, filer)
	}

	if crashLogFile != "" {
		f, err := os.OpenFile(crashLogFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		syscall.Dup2(int(f.Fd()), int(os.Stdout.Fd()))
		syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
		fmt.Fprintf(os.Stderr, "\n%s %s (build: %s)\n===================\n",
			time.Now().String(),
			gafka.Version, gafka.BuildId)
	}

}
