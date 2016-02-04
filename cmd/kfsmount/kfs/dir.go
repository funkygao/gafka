package kfs

import (
	"log"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
)

type Dir struct {
	sync.RWMutex
	attr fuse.Attr

	path string
	fs   *KafkaFS
}

func (d *Dir) Attr(ctx context.Context, o *fuse.Attr) error {
	d.RLock()
	defer d.RUnlock()

	*o = d.attr

	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.RLock()
	defer d.RUnlock()

	log.Printf("[Dir] Lookup, name=%s", name)

	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.RLock()
	defer d.RUnlock()

	var out []fuse.Dirent

	kfk, err := sarama.NewClient(d.fs.zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Println(err)

		return nil, err
	}
	defer kfk.Close()

	topics, err := kfk.Topics()
	if err != nil {
		log.Println(err)

		return nil, err
	}

	for _, topic := range topics {
		de := fuse.Dirent{
			Name: topic,
			Type: fuse.DT_File,
		}

		out = append(out, de)
	}

	return out, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	return nil, fuse.EPERM
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, fuse.EPERM
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	return fuse.EPERM
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return fuse.EPERM
}
