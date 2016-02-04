package kfs

import (
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
	sync.RWMutex
	attr fuse.Attr

	path   string
	fs     *KafkaFS
	parent *Dir
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
	log.Printf("(*Dir) Lookup, name=%s", name)

	// n, exist := d.nodes[name]
	path := filepath.Join(d.path, name)
	stats, err := os.Stat(path)
	if err != nil {
		//The real file does not exists.
		log.Printf("(*Dir) ERRO arquivo real nao existe, name=%s", name)
		return nil, fuse.ENOENT
	}

	switch {
	case stats.IsDir():
		return d.fs.newDir(path, stats.Mode()), nil
	case stats.Mode().IsRegular():
		return d.fs.newFile(path, stats.Mode()), nil
	default:
		panic("Unknown type in filesystem")
	}
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
	d.Lock()
	defer d.Unlock()

	log.Printf("(*Dir) Mkdir, name=%s", req.Name)

	if exists := d.exists(req.Name); exists {
		log.Println("Mkdir ERR: EEXIST")
		return nil, fuse.EEXIST
	}

	path := filepath.Join(d.path, req.Name)
	n := d.fs.newDir(path, req.Mode)

	if err := os.Mkdir(path, req.Mode); err != nil {
		log.Println("Mkdir ERR:  ", err)
		return nil, err
	}
	return n, nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.Lock()
	defer d.Unlock()

	log.Printf("(*Dir) Create, name=%s", req.Name)

	if exists := d.exists(req.Name); exists {
		log.Println("Create open ERR: EEXIST")
		return nil, nil, fuse.EEXIST
	}
	path := filepath.Join(d.path, req.Name)
	fHandler, err := os.OpenFile(path, int(req.Flags), req.Mode)
	if err != nil {
		log.Println("Create open ERR: ", err)
		return nil, nil, err
	}

	n := d.fs.newFile(path, req.Mode)
	n.fs = d.fs
	n.handler = fHandler

	resp.Attr = n.attr

	return n, n, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	log.Printf("(*Dir) Rename, name=%q", req)

	nd := newDir.(*Dir)
	if d.attr.Inode == nd.attr.Inode {
		d.Lock()
		defer d.Unlock()
	} else if d.attr.Inode < nd.attr.Inode {
		d.Lock()
		defer d.Unlock()
		nd.Lock()
		defer nd.Unlock()
	} else {
		nd.Lock()
		defer nd.Unlock()
		d.Lock()
		defer d.Unlock()
	}

	if exists := d.exists(req.OldName); !exists {
		log.Println("Rename ERR: ENOENT")
		return fuse.ENOENT
	}

	oldPath := filepath.Join(d.path, req.OldName)
	newPath := filepath.Join(nd.path, req.NewName)

	if err := os.Rename(oldPath, newPath); err != nil {
		log.Println("Rename ERR: ", err)
		return err
	}
	return nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.Lock()
	defer d.Unlock()

	log.Printf("(*Dir) Remove, req=%s, path=%s, name=%s", req, filepath.Base(d.path), req.Name)

	if exists := d.exists(req.Name); !exists {
		log.Println("Remove ERR: ENOENT")
		return fuse.ENOENT
	}

	// Para remover subdiretorios
	// else if hasChildren() {
	// 	log.Println("Remove ERR: ENOENT")
	// 	return fuse.ENOENT
	// }

	path := filepath.Join(d.path, req.Name)
	if err := os.Remove(path); err != nil {
		log.Println("Remove ERR: ", err)
		return err
	}
	return nil
}

func (d *Dir) exists(name string) bool {
	path := filepath.Join(d.path, name)
	_, err := os.Stat(path)
	if err != nil {
		return false
	}

	return true
}
