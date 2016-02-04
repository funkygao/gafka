package kfs

import (
	"io"
	"log"
	"os"
	"path/filepath"
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

	fs      *KafkaFS
	handler *os.File

	topic       string
	partitionId int32

	path string // TODO
}

func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	defer f.RUnlock()

	log.Printf("File Attr, topic=%s, partitionId:%s", f.topic, f.partitionId)

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

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Printf("(*File) Open, req=%q, path=%s", req, filepath.Base(f.path))

	fsHandler, err := os.OpenFile(f.path, int(req.Flags), f.attr.Mode)
	if err != nil {
		log.Print("Open ERR: ", err)
		return nil, err
	}
	f.handler = fsHandler

	// resp.Flags |= fuse.OpenDirectIO

	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Printf("(*File) Release, req=%q, path=%s", req, filepath.Base(f.path))
	return f.handler.Close()
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.RLock()
	defer f.RUnlock()

	log.Printf("(*File) Read, req=%q, path=%s", req, filepath.Base(f.path))

	if f.handler == nil {
		log.Println("Read: File should be open, aborting request")
		return fuse.ENOTSUP
	}

	resp.Data = resp.Data[:req.Size]
	n, err := f.handler.ReadAt(resp.Data, req.Offset)
	if err != nil && err != io.EOF {
		log.Println("Read ERR: ", err)
		return err
	}
	resp.Data = resp.Data[:n]

	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.Lock()
	defer f.Unlock()

	log.Printf("(*File) Write, req=%q, path=%s", req, filepath.Base(f.path))

	if f.handler == nil {
		log.Println("Write: File should be open, aborting request")
		return fuse.ENOTSUP
	}

	n, err := f.handler.WriteAt(req.Data, req.Offset)
	if err != nil {
		log.Println("Write ERR: ", err)
		return err
	}
	resp.Size = n

	return nil
}
