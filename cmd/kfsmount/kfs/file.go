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

	opened  bool
	content []byte
	closeCh chan struct{}

	topic       string
	partitionId int32
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	defer f.RUnlock()

	*o = f.attr

	// calculate size
	if !f.opened {
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

		o.Size = uint64(latestOffset - oldestOffset)
	} else {
		o.Size = uint64(len(f.content))
	}

	log.Trace("File Attr, topic=%s, partitionId=%d, size=%d", f.topic, f.partitionId, o.Size)

	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest,
	resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Trace("File Open, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	// Allow kernel to use buffer cache
	resp.Flags &^= fuse.OpenDirectIO
	f.opened = true
	f.closeCh = make(chan struct{})

	go f.readContent()
	time.Sleep(time.Second * 2) // TODO

	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Trace("File Release, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)
	f.opened = false
	close(f.closeCh)
	f.content = make([]byte, 0, 16<<10)
	return f.consumer.Close()
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	f.RLock()
	defer f.RUnlock()

	log.Trace("File ReadAll, topic=%s, partitionId=%d", f.topic, f.partitionId)

	return f.content, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.RLock()
	defer f.RUnlock()

	log.Trace("File Read, req=%#v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	resp.Data = resp.Data[:req.Size]
	n := len(f.content) - int(req.Offset)
	resp.Data = f.content[req.Offset:]
	resp.Data = resp.Data[:n]

	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return fuse.EPERM
}

func (f *File) readContent() {
	if err := f.reconsume(sarama.OffsetOldest); err != nil {
		log.Error(err)
		return
	}

	var msg *sarama.ConsumerMessage
	for {
		select {
		case <-f.closeCh:
			return

		case msg = <-f.consumer.Messages():
			f.content = append(f.content, msg.Value...)
			f.content = append(f.content, '\n')
		}
	}
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
