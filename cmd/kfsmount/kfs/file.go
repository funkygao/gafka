package kfs

import (
	"log"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/Shopify/sarama"
	"golang.org/x/net/context"
)

type File struct {
	sync.RWMutex
	attr fuse.Attr

	fs       *KafkaFS
	consumer sarama.Consumer

	topic       string
	partitionId int32
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	defer f.RUnlock()

	log.Printf("File Attr, topic=%s, partitionId=%d", f.topic, f.partitionId)

	kfk, err := sarama.NewClient(f.fs.zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Println(err)

		return err
	}
	defer kfk.Close()

	latestOffset, err := kfk.GetOffset(f.topic, f.partitionId, sarama.OffsetNewest)
	if err != nil {
		log.Println(err)

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
	log.Printf("File Open, req=%+v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(f.fs.zkcluster.BrokerList(), config)
	if err != nil {
		log.Println(err)

		return nil, err
	}

	f.consumer = consumer

	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Printf("File Release, req=%+v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)
	return f.consumer.Close()
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.RLock()
	defer f.RUnlock()

	log.Printf("File Read, req=%+v, topic=%s, partitionId=%d", req,
		f.topic, f.partitionId)

	if f.consumer == nil {
		log.Println("Read: Consumer should be open, aborting request")
		return fuse.ENOTSUP
	}

	// TODO req.Size, req.Offset
	resp.Data = []byte{}

	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	return fuse.EPERM
}
