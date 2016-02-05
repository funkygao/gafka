package kfs

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"golang.org/x/net/context"
)

type KafkaFS struct {
	zkcluster *zk.ZkCluster
	root      *Dir

	// id generator
	inodeId uint64

	size int64
}

func New(zone, cluster string) *KafkaFS {
	log.Info("starting kfs zone:%s, cluster:%s", zone, cluster)

	this := &KafkaFS{}

	ctx.LoadFromHome()
	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.zkcluster = zkzone.NewCluster(cluster) // panic if invalid cluster

	this.root = this.newDir(os.FileMode(0777))
	if this.root.attr.Inode != 1 {
		panic("Root must receive inode 1")
	}
	return this
}

func (this *KafkaFS) Root() (fs.Node, error) {
	return this.root, nil
}

func (this *KafkaFS) nextInodeId() uint64 {
	return atomic.AddUint64(&this.inodeId, 1)
}

func (this *KafkaFS) newDir(mode os.FileMode) *Dir {
	now := time.Now()
	return &Dir{
		attr: fuse.Attr{
			Inode:  this.nextInodeId(),
			Atime:  now,
			Mtime:  now,
			Ctime:  now,
			Crtime: now,
			Mode:   os.ModeDir | mode,
		},
		fs: this,
	}
}

func (this *KafkaFS) newFile(dir *Dir, topic string, partitionId int32, mode os.FileMode) *File {
	now := time.Now()
	return &File{
		attr: fuse.Attr{
			Inode:  this.nextInodeId(),
			Atime:  now,
			Mtime:  now,
			Ctime:  now,
			Crtime: now,
			Mode:   mode,
		},
		fs:          this,
		dir:         dir,
		topic:       topic,
		partitionId: partitionId,
	}
}

func (this *KafkaFS) Statfs(ctx context.Context, req *fuse.StatfsRequest,
	res *fuse.StatfsResponse) error {
	s := syscall.Statfs_t{}
	res.Blocks = s.Blocks
	res.Bfree = s.Bfree
	res.Bavail = s.Bavail
	res.Ffree = s.Ffree
	res.Bsize = uint32(s.Bsize)
	return nil
}
