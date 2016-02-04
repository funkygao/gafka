package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kfsmount/kfs"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

func init() {
	parseFlags()

	if options.version {
		fmt.Fprintf(os.Stderr, "%s-%s\n", gafka.Version, gafka.BuildId)
		os.Exit(0)
	}

	if options.boot {
		fmt.Printf("yum install -y fuse\nmodprobe fuse\nlsmod | grep fuse\n")
		os.Exit(0)
	}
}

func main() {
	validateFlags()
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	setupLogging("stdout", options.logLevel, "")

	c, err := fuse.Mount(
		options.mount,
		fuse.FSName("kfs"),
		fuse.Subtype("kfs"),
		fuse.VolumeName("Kafka FS"),
		fuse.ReadOnly(),
		fuse.AllowOther(),
	)
	if err != nil {
		log.Critical(err)
	}
	defer c.Close()

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		for i := 0; i < 10; i++ {
			err := fuse.Unmount(options.mount)
			if err == nil {
				break
			}

			log.Warn(err)
			time.Sleep(time.Second * 5)
		}
	}, syscall.SIGINT, syscall.SIGTERM)

	srv := fs.New(c, &fs.Config{})
	fs := kfs.New(options.zone, options.cluster)

	if err := srv.Serve(fs); err != nil {
		log.Error(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Error(err)
	}

	log.Info("Kafka FS unmounted")
}
