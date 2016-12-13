package command

import (
	"flag"
	"fmt"
	"path"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Rm struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	path      string
	recursive bool
	likeMode  bool
}

func (this *Rm) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("rm", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	cmdFlags.BoolVar(&this.likeMode, "like", false, "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-p").
		requireAdminRights("-p").
		invalid(args) {
		return 2
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()
	if this.recursive {
		if this.likeMode {
			parent := path.Dir(this.path)
			for p := range zkzone.ChildrenWithData(parent) {
				realPath := parent + p
				if patternMatched(realPath, this.path) {
					this.Ui.Warn(fmt.Sprintf("deleting %s", realPath))

					err := zkzone.DeleteRecursive(realPath)
					if err != nil {
						this.Ui.Error(err.Error())
					} else {
						this.Ui.Info(fmt.Sprintf("%s deleted ok", realPath))
					}
				}
			}

			return
		}

		// not like mode
		err := zkzone.DeleteRecursive(this.path)
		if err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Info(fmt.Sprintf("%s deleted ok", this.path))
		}

		return 0
	}

	// remove a single znode
	conn := zkzone.Conn()
	err := conn.Delete(this.path, -1)
	if err != nil {
		this.Ui.Error(err.Error())
	} else {
		this.Ui.Info(fmt.Sprintf("%s deleted ok", this.path))
	}

	return
}

func (*Rm) Synopsis() string {
	return "Remove znode"
}

func (this *Rm) Help() string {
	help := fmt.Sprintf(`
Usage: %s rm -z zone -p path [options]

    Remove znode

Options:

    -R
      Recursively remove subdirectories encountered.

    -like
      Pattern matching mode.

`, this.Cmd)
	return strings.TrimSpace(help)
}
