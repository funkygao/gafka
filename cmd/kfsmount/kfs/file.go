package kfs

import (
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"golang.org/x/net/context"
)

type File struct {
	sync.RWMutex
	attr fuse.Attr

	fs       *KafkaFS
	consumer sarama.PartitionConsumer

	topic       string
	partitionId int32
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	defer f.RUnlock()

	log.Trace("File Attr, topic=%s, partitionId=%d", f.topic, f.partitionId)

	kfk, err := sarama.NewClient(f.fs.zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Error(err)

		return err
	}
	defer kfk.Close()

	latestOffset, err := kfk.GetOffset(f.topic, f.partitionId, sarama.OffsetNewest)
	if err != nil {
		log.Error(err)

		return err
	}

	now := time.Now()
	*o = fuse.Attr{
		Mode:  0555,
		Size:  uint64(latestOffset),
		Mtime: now,
		Ctime: now,
	}

	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest,
	resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Trace("File Open, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	if err := f.reconsume(sarama.OffsetOldest); err != nil {
		return nil, err
	}

	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Trace("File Release, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)
	return f.consumer.Close()
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.RLock()
	defer f.RUnlock()

	log.Trace("File Read, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	if f.consumer == nil {
		log.Error("Read: Consumer should be open, aborting request")
		return fuse.ENOTSUP
	}

	// TODO req.Size, req.Offset
	msg := <-f.consumer.Messages()
	resp.Data = msg.Value

	return nil
}

func (f *File) reconsume(offset int64) error {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(f.fs.zkcluster.BrokerList(), config)
	if err != nil {
		log.Error(err)

		return err
	}

	p, err := consumer.ConsumePartition(f.topic, f.partitionId, offset)
	if err != nil {
		log.Error(err)

		return err
	}

	f.consumer = p
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return fuse.EPERM
}
