package main

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/funkygao/gafka/cmd/kfsmount/kfs"
)

func init() {
	parseFlags()

	if options.boot {
		fmt.Printf("yum install fuse\nmodprobe fuse\nlsmod | grep fuse")
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
	defer func() {
		c.Close()
		fuse.Unmount(options.mount)
	}()

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
