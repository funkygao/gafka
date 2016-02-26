package command

import (
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type argsRule struct {
	cmd        cli.Command
	ui         cli.Ui
	requires   []string
	conditions map[string][]string
}

func validateArgs(cmd cli.Command, ui cli.Ui) *argsRule {
	return &argsRule{
		cmd:        cmd,
		ui:         ui,
		requires:   make([]string, 0),
		conditions: make(map[string][]string),
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

	return false
}

func patternMatched(s, pattern string) bool {
	if pattern != "" {
		if pattern[0] == '~' {
			// NOT
			return !strings.Contains(s, pattern[1:])
		}

		if !strings.Contains(s, pattern) {
			return false
		}
	}

	return true
}
