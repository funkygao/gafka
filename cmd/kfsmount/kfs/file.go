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
	dir      *Dir
	consumer sarama.PartitionConsumer

	topic       string
	partitionId int32
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	defer f.RUnlock()

	log.Trace("File Attr, topic=%s, partitionId=%d", f.topic, f.partitionId)

	if err := f.dir.reconnectKafkaIfNecessary(); err != nil {
		return err
	}

	latestOffset, err := f.dir.GetOffset(f.topic, f.partitionId, sarama.OffsetNewest)
	if err != nil {
		log.Error(err)

		return err
	}
	oldestOffset, err := f.dir.GetOffset(f.topic, f.partitionId, sarama.OffsetOldest)
	if err != nil {
		log.Error(err)

		return err
	}

	now := time.Now()
	*o = fuse.Attr{
		Mode:  0555,
		Size:  uint64(latestOffset - oldestOffset),
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
	offset := 0
	resp.Data = resp.Data[:req.Size]
	for {
		select {
		case msg := <-f.consumer.Messages():
			if offset+len(msg.Value) > req.Size {
				return nil
			}

			log.Trace("offset: %d, msg: %s, data: %s", offset, string(msg.Value), string(resp.Data))

			copy(resp.Data[offset:], msg.Value)
			offset += len(msg.Value)

		case <-time.After(time.Second * 5):
			return nil
		}

	}

	return nil
}

func (f *File) reconsume(offset int64) error {
	if err := f.dir.reconnectKafkaIfNecessary(); err != nil {
		return err
	}

	consumer, err := sarama.NewConsumerFromClient(f.dir.Client)
	if err != nil {
		log.Error(err)

		return err
	}

	cp, err := consumer.ConsumePartition(f.topic, f.partitionId, offset)
	if err != nil {
		log.Error(err)

		return err
	}

	f.consumer = cp
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return fuse.EPERM
}
