package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type AuthFunc func(user, pass string) bool

const (
	adminPasswd = "gAfKa"
)

var (
	Authenticator AuthFunc
)

func refreshScreen() {
	c := exec.Command("clear")
	c.Stdout = os.Stdout
	c.Run()
}

func init() {
	// default auth component, caller can override this
	Authenticator = func(user, pass string) bool {
		if pass == adminPasswd {
			return true
		}

		return false
	}
}

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

func inData() []byte {
	data, _ := ioutil.ReadAll(os.Stdin)
	return data
}

func must(err error) {
	if err != nil {
		errString := strings.TrimPrefix(err.Error(), "zk: ")
		fmt.Fprintf(os.Stderr, "error: %s\n", errString)
		os.Exit(1)
	}
}

func patternMatched(s, pattern string) bool {
	if pattern != "" && !strings.Contains(s, pattern) {
		return false
	}

	return true
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
