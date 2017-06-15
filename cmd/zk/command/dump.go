package command

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/samuel/go-zookeeper/zk"
)

type Dump struct {
	Ui  cli.Ui
	Cmd string

	zone       string
	path       string
	importFile string
	infile     string
	outfile    string
	outdir     string
	f          *os.File
}

func (this *Dump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("dump", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.outfile, "o", "zk.dump", "")
	cmdFlags.StringVar(&this.infile, "in", "", "")
	cmdFlags.StringVar(&this.importFile, "import", "", "")
	cmdFlags.StringVar(&this.path, "p", "/", "")
	cmdFlags.StringVar(&this.outdir, "dir", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		requireAdminRights("-import").
		invalid(args) {
		return 2
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	if this.importFile != "" {
		if this.path == "" {
			this.Ui.Error("-p required")
			return 2
		}

		zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
		defer zkzone.Close()

		this.importFromFile(zkzone)
		return
	}

	if this.infile != "" {
		// display mode
		this.diplayDumppedFile()
		return
	}

	// dump mode
	this.outfile = this.zone + "." + this.outfile

	var err error
	if this.outdir != "" {
		err = os.MkdirAll(this.outdir, 0755)
		must(err)

		this.outfile = fmt.Sprintf("%s/%s", this.outdir, this.outfile)

		_, err = os.Lstat(this.outfile)
		if err == nil {
			// file exists, find next available number
			num := 1
			fname := ""
			for ; err == nil && num <= 9999; num++ { // 30 year is enough
				fname = this.outfile + fmt.Sprintf(".%04d", num)
				_, err = os.Lstat(fname)
			}
			if err == nil {
				panic("Not able to rotate, 30 years passed?")
			}

			err = os.Rename(this.outfile, fname)
			must(err)

			this.Ui.Info(fmt.Sprintf("rename %s -> %s", this.outfile, fname))
		}
	}

	this.f, err = os.OpenFile(this.outfile,
		os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	must(err)

	zkzone := gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	defer zkzone.Close()

	this.dump(zkzone.Conn(), this.path)
	this.f.Close()

	this.Ui.Info(fmt.Sprintf("dumpped to %s", this.outfile))

	return
}

func (this *Dump) importFromFile(zkzone *gzk.ZkZone) {
	f, err := os.Open(this.importFile)
	must(err)

	for {
		// read line, got the znode path
		var buf [1]byte
		zpath := make([]byte, 0, 8<<10)
		for {
			b := buf[:]
			_, err := f.Read(b)
			if err == io.EOF {
				return
			}
			must(err)

			if b[0] == '\n' {
				break
			}
			zpath = append(zpath, b[0])
		}

		newPath := path.Join(this.path, string(zpath))
		this.Ui.Infof("%50s -> [%s]%s", string(zpath), this.zone, newPath)

		// read the znode data
		// 1. data len
		// 2. data itself
		var dataLen int32
		err = binary.Read(f, binary.BigEndian, &dataLen)
		must(err)

		zdata := make([]byte, dataLen)
		_, err = io.ReadFull(f, zdata)
		must(err)

		must(zkzone.CreatePermenantZnode(newPath, zdata))
	}
}

func (this *Dump) diplayDumppedFile() {
	f, err := os.Open(this.infile)
	must(err)

	for {
		// read line, got the znode path
		var buf [1]byte
		zpath := make([]byte, 0, 8<<10)
		for {
			b := buf[:]
			_, err := f.Read(b)
			if err == io.EOF {
				return
			}
			must(err)

			if b[0] == '\n' {
				break
			}
			zpath = append(zpath, b[0])
		}

		this.Ui.Info(string(zpath))

		// read the znode data
		// 1. data len
		// 2. data itself
		var dataLen int32
		err = binary.Read(f, binary.BigEndian, &dataLen)
		must(err)

		zdata := make([]byte, dataLen)
		_, err = io.ReadFull(f, zdata)
		must(err)

		this.Ui.Output(string(zdata))
	}
}

// serialization format:
// path
// len(4) value
func (this *Dump) dump(conn *zk.Conn, path string) {
	children, _, err := conn.Children(path)
	if err != nil {
		must(err)
		return
	}

	sort.Strings(children)
	var buf [4]byte
	for _, child := range children {
		if path == "/" {
			path = ""
		}

		znode := fmt.Sprintf("%s/%s", path, child)

		// display znode content
		data, stat, err := conn.Get(znode)
		must(err)
		if stat.EphemeralOwner > 0 {
			// ignore ephemeral znodes
			continue
		}

		_, err = this.f.Write([]byte(znode))
		must(err)
		_, err = this.f.Write([]byte{'\n'})
		must(err)
		v := buf[0:4]
		binary.BigEndian.PutUint32(v, uint32(len(data)))
		_, err = this.f.Write(v)

		if len(data) > 0 {
			_, err = this.f.Write(data)
			must(err)
		}

		this.dump(conn, znode)
	}
}

func (*Dump) Synopsis() string {
	return "Dump/Import permanent znodes of Zookeeper"
}

func (this *Dump) Help() string {
	help := fmt.Sprintf(`
Usage: %s dump -z zone [options]

    %s

Options:

    -p path 
      znode root path

    -o outfile
      Default zk.dump
      zone name will automatically prefix the final outfile.

    -import dumped file name
      e,g.
      zk dump -z test -p /xxx/yyy
      zk dump -import test.zk.dump -z prod -p /
      this will clone /xxx/yyy of test zone into prod zone.

    -in dumpped input filename
      Display dumpped file contents in text format.

    -dir dir name
      Run daily dump to this directoy. 
      zk will automatically rotate target dumps output.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
