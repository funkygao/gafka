package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kfsmount/kfs"
	"github.com/funkygao/golib/signal"
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

	c, err := fuse.Mount(
		options.mount,
		fuse.FSName("kfs"),
		fuse.Subtype("kfs"),
		fuse.VolumeName("Kafka FS"),
		fuse.ReadOnly(),
		fuse.AllowOther(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		fuse.Unmount(options.mount)
	}, syscall.SIGINT, syscall.SIGTERM)

	srv := fs.New(c, &fs.Config{})
	fs := kfs.New(options.zone, options.cluster)

	if err := srv.Serve(fs); err != nil {
		log.Fatal(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}

	log.Println("Kafka FS unmounted")
}
