package command

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/merkle"
)

type Merkle struct {
	Ui  cli.Ui
	Cmd string

	root string
}

func (this *Merkle) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("merkle", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing path")
		return 2
	}

	this.root = args[len(args)-1]
	if _, err := os.Stat(this.root); err != nil {
		this.Ui.Error(err.Error())
		return 1
	}

	this.showHash()
	return
}

func (this *Merkle) showHash() {
	var blocks [][]byte
	swallow(filepath.Walk(this.root, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		data, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		blocks = append(blocks, data)
		return nil
	}))

	tree := merkle.NewTree()
	swallow(tree.Generate(blocks, md5.New()))

	this.Ui.Outputf("height: %d", tree.Height())
	for h := 1; h <= int(tree.Height()); h++ {
		format := fmt.Sprintf("%%%ds ", (h-1)*4)
		format += "%+v"
		nodes := tree.GetNodesAtHeight(uint64(h))
		for _, node := range nodes {
			this.Ui.Outputf(format, "", node.Hash)
		}
	}

}

func (*Merkle) Synopsis() string {
	return "Recursively calculate hash of directory with merkle tree"
}

func (this *Merkle) Help() string {
	help := fmt.Sprintf(`
Usage: %s merkle path

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
