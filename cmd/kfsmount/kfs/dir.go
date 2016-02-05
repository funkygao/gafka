package kfs

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"golang.org/x/net/context"
)

type Dir struct {
	fs *KafkaFS
	sarama.Client

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

	log.Trace("Dir Lookup, name=%s", name)

	// split the name into topic and partitionId
	partitionOffset := -1
	for i := len(name) - 1; i > 0; i-- {
		if name[i] == '.' {
			partitionOffset = i
		}
	}
	if partitionOffset == -1 {
		return nil, fuse.ENOENT
	}

	topic := name[:partitionOffset]
	partitionId, _ := strconv.Atoi(name[partitionOffset+1:])

	return d.fs.newFile(d, topic, int32(partitionId), os.FileMode(0555)), nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.RLock()
	defer d.RUnlock()

	var out []fuse.Dirent
	if err := d.reconnectKafkaIfNecessary(); err != nil {
		return nil, err
	}

	topics, err := d.Topics()
	if err != nil {
		log.Error(err)

		return nil, err
	}

	for _, topic := range topics {
		partitions, err := d.Partitions(topic)
		if err != nil {
			log.Error(err)

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

// TODO when to close the kafka client?
func (d *Dir) reconnectKafkaIfNecessary() error {
	if d.Client != nil {
		return nil
	}

	kfk, err := sarama.NewClient(d.fs.zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Error(err)

		return err
	}

	d.Client = kfk
	log.Trace("kafka %+v connected", d.fs.zkcluster.BrokerList())
	return nil
}
