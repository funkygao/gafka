package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Farm struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Farm) Run(args []string) (exitCode int) {
	var (
		zone    string
		addNode string
		tag     string
	)
	cmdFlags := flag.NewFlagSet("farm", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&addNode, "add", "", "")
	cmdFlags.StringVar(&tag, "tag", "", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	switch {
	case addNode != "":
		if err := zkzone.EnsurePathExists(fmt.Sprintf("/_farm/%s", addNode)); err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Infof("%s registered", addNode)
		}

	case tag != "":
		tuples := strings.Split(tag, "+")
		path := fmt.Sprintf("/_farm/%s", tuples[0])
		data, _, err := zkzone.Conn().Get(path)
		swallow(err)
		var tags []string
		err = json.Unmarshal(data, &tags)
		swallow(err)
		tags = append(tags, tuples[1])
		data, err = json.Marshal(tags)
		swallow(err)
		zkzone.Conn().Set(path, data, -1)

	default:
		children := zkzone.ChildrenWithData("/_farm")
		sortedChildren := make([]string, 0, len(children))
		for c := range children {
			sortedChildren = append(sortedChildren, c)
		}
		sort.Strings(sortedChildren)

		for _, c := range sortedChildren {
			data := children[c]
			var v []string
			json.Unmarshal(data.Data(), &v)
			this.Ui.Outputf("%20s %+v", c, v)
		}

	}

	return
}

func (*Farm) Synopsis() string {
	return "Manage reserved servers farm to deploy new kafka brokers"
}

func (this *Farm) Help() string {
	help := fmt.Sprintf(`
Usage: %s farm [options]

    %s

Options:

    -add ip
     Add a new server to the server farm
    
    -tag ip+tag
     Add a tag.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
