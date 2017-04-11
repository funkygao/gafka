package command

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
	"github.com/funkygao/zkclient"
	"github.com/samuel/go-zookeeper/zk"
)

type Get struct {
	Ui  cli.Ui
	Cmd string

	zone        string
	path        string
	verbose     bool
	recursive   bool
	watch       bool
	pretty      bool
	urlDecode   bool
	likePattern string

	zc *zkclient.Client
}

func (this *Get) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("get", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.BoolVar(&this.verbose, "l", false, "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	cmdFlags.BoolVar(&this.urlDecode, "d", false, "")
	cmdFlags.BoolVar(&this.pretty, "pretty", false, "")
	cmdFlags.BoolVar(&this.watch, "w", false, "")
	cmdFlags.StringVar(&this.likePattern, "like", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.path = args[len(args)-1]

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	if this.recursive {
		data, stat, err := zkzone.Conn().Get(this.path)
		must(err)
		if stat.EphemeralOwner > 0 {
			this.Ui.Output(color.Yellow(this.path))
		} else {
			this.Ui.Output(color.Green(this.path))
		}

		if len(data) > 0 {
			if this.verbose {
				this.Ui.Output(fmt.Sprintf("%s %#v",
					strings.Repeat(" ", 3), stat))
				this.Ui.Output(fmt.Sprintf("%s %v",
					strings.Repeat(" ", 3), data))
			}
			this.Ui.Output(fmt.Sprintf("%s %s",
				strings.Repeat(" ", 3), this.convertZdata(data)))
		}

		this.showChildrenRecursively(zkzone.Conn(), this.path)

		return 0
	}

	conn := zkzone.Conn()
	data, stat, err := conn.Get(this.path)
	must(err)

	if this.watch {
		this.Ui.Outputf("%s: %s", this.path, this.convertZdata(data))

		this.watchZnode(zkzone.ZkAddrs())
		select {}
		return
	}

	if len(data) == 0 {
		this.Ui.Output("empty znode")
		return
	}

	if this.verbose {
		this.Ui.Output(color.Magenta("%#v", *stat))
	}

	this.Ui.Output(color.Green("Data Bytes"))
	fmt.Println(data)
	this.Ui.Output(color.Green("Data as String"))
	this.Ui.Output(this.convertZdata(data))

	return
}

func (this *Get) watchZnode(zkConnStr string) {
	zc := zkclient.New(zkConnStr)
	must(zc.Connect())
	this.zc = zc

	zc.SubscribeDataChanges(this.path, this)
}

func (this *Get) HandleDataChange(dataPath string, data []byte) error {
	data, err := this.zc.Get(dataPath)
	must(err)
	this.Ui.Outputf("%s->%s", dataPath, string(data))
	return nil
}

func (this *Get) HandleDataDeleted(dataPath string) error {
	this.Ui.Errorf("%s deleted", dataPath)
	return nil
}

func (this *Get) showChildrenRecursively(conn *zk.Conn, path string) {
	children, _, err := conn.Children(path)
	if err != nil {
		return
	}

	sort.Strings(children)
	for _, child := range children {
		if path == "/" {
			path = ""
		}

		znode := fmt.Sprintf("%s/%s", path, child)

		// display znode content
		data, stat, err := conn.Get(znode)
		must(err)

		if this.urlDecode {
			if u, e := url.Parse(znode); e == nil {
				znode = u.Path
			}
		}

		if stat.EphemeralOwner > 0 {
			if patternMatched(znode, this.likePattern) {
				this.Ui.Output(color.Yellow(znode))
			}
		} else {
			if patternMatched(znode, this.likePattern) {
				this.Ui.Output(color.Green(znode))
			}
		}

		if len(data) > 0 && patternMatched(znode, this.likePattern) {
			if this.verbose {
				this.Ui.Output(fmt.Sprintf("%s %#v",
					strings.Repeat(" ", 3), stat))
				this.Ui.Output(fmt.Sprintf("%s %v",
					strings.Repeat(" ", 3), data))
			}
			this.Ui.Output(fmt.Sprintf("%s %s",
				strings.Repeat(" ", 3), this.convertZdata(data)))
		}

		this.showChildrenRecursively(conn, znode)
	}
}

func (this *Get) convertZdata(data []byte) string {
	if !this.pretty {
		return string(data)
	}

	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, data, "    ", "    "); err == nil {
		// it's a valid json
		return prettyJSON.String()
	}

	return string(data)
}

func (*Get) Synopsis() string {
	return "Show znode data"
}

func (this *Get) Help() string {
	help := fmt.Sprintf(`
Usage: %s get [options] <path>

    %s

Options:

    -z zone
    
    -R
      Recursively show subdirectories znode encountered.

    -w
      Watch data changes.

    -d
      URLDecode the znode path.

    -l
      Use a long display format.

    -pretty
      Try pretty print znode json data.

    -like pattern
      Only display znode whose path is like this pattern.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
