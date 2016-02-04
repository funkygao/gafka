package kfs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
)

type Dir struct {
	fs *KafkaFS

	sync.RWMutex
	attr fuse.Attr
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

	log.Printf("Dir Lookup, name=%s", name)

	return d.fs.newFile(filepath.Join(d.fs.zkcluster.Name(), name), os.FileMode(0777)), nil
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
		partitions, err := kfk.Partitions(topic)
		if err != nil {
			log.Println(err)

			return nil, err
		}

		for _, p := range partitions {
			de := fuse.Dirent{
				Name: fmt.Sprintf("%s.%d", topic, p),
				Type: fuse.DT_File,
			}

			out = append(out, de)
		}

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
