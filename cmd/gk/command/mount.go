package command

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/funkygao/gafka/cmd/gk/command/kfs"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

type Mount struct {
	Ui  cli.Ui
	Cmd string

	zone       string
	cluster    string
	logLevel   string
	mountPoint string
}

func (this *Mount) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("mount", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.logLevel, "l", "info", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		on("-z", "-c").
		invalid(args) {
		return 2
	}

	this.mountPoint = args[len(args)-1]
	if !strings.HasPrefix(this.mountPoint, "/") {
		this.Ui.Error("mount point must start with /")
		return 1
	}

	setupLogging("stdout", this.logLevel, "")

	c, err := fuse.Mount(
		this.mountPoint,
		fuse.FSName("kfs"),
		fuse.Subtype("kfs"),
		fuse.VolumeName("Kafka FS"),
		fuse.ReadOnly(),
		fuse.AllowOther(),
	)
	if err != nil {
		log.Critical(err)
	}

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		var err error
		for i := 0; i < 5; i++ {
			err = fuse.Unmount(this.mountPoint)
			if err == nil {
				break
			}

			log.Warn(err)
			time.Sleep(time.Second * 5)
		}

		if err == nil {
			log.Info("Kafka FS unmounted")
		} else {
			log.Error("Kafka FS unable to umount")
		}

		c.Close()
		os.Exit(0)
	}, syscall.SIGINT, syscall.SIGTERM)

	srv := fs.New(c, &fs.Config{})
	fs := kfs.New(this.zone, this.cluster)

	if err := srv.Serve(fs); err != nil {
		log.Error(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Error(err)
	}

	return
}

func (*Mount) Synopsis() string {
	return "A FUSE module to mount a Kafka cluster in the filesystem"
}

func (this *Mount) Help() string {
	help := fmt.Sprintf(`
Usage: %s mount [options] mount_point

    A FUSE module to mount a Kafka cluster in the filesystem

Pre-requisites:
    yum install -y fuse
    modprobe fuse
    lsmod | grep fuse

Options:

    -z zone
      Default '%s'.

    -c cluster

    -l log level
      Default 'info'.
      

`, this.Cmd, ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
