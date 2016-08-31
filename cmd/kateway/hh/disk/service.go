package disk

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	gio "github.com/funkygao/golib/io"
)

type DiskService struct {
	cfg *Config

	quiting chan struct{}
	rwmux   sync.RWMutex
	wg      sync.WaitGroup

	// hh
	// ├── cluster1
	// └── cluster2
	//     ├── topic1
	//     └── topic2
	//         ├── 1
	//         ├── 2
	//         ├── 3
	//         └── cursor.dmp
	queues map[string]map[string]*queue // cluster:topic->queue
}

func New(cfg *Config) *DiskService {
	return &DiskService{
		cfg:     cfg,
		quiting: make(chan struct{}),
		queues:  make(map[string]map[string]*queue),
	}
}

func (this *DiskService) Start() error {
	if !gio.DirExists(this.cfg.Dir) {
		if err := os.Mkdir(this.cfg.Dir, 0700); err != nil {
			return err
		}
	}

	clusters, err := ioutil.ReadDir(this.cfg.Dir)
	if err != nil {
		return err
	}

	// load queues from disk
	for _, cluster := range clusters {
		if !cluster.IsDir() {
			continue
		}

		this.queues[cluster.Name()] = make(map[string]*queue)
		topicDir := filepath.Join(this.cfg.Dir, cluster.Name())
		topics, err := ioutil.ReadDir(topicDir)
		if err != nil {
			return err
		}

		for _, topic := range topics {
			if !topic.IsDir() {
				continue
			}

			this.queues[cluster.Name()][topic.Name()] = newQueue(
				cluster.Name(), topic.Name(),
				filepath.Join(topicDir, topic.Name()), -1)
			if err = this.queues[cluster.Name()][topic.Name()].Open(); err != nil {
				return err
			}

			this.wg.Add(1)
			go this.queues[cluster.Name()][topic.Name()].housekeeping(this.cfg.PurgeInterval, &this.wg)
		}
	}

	return nil
}

func (this *DiskService) Cursor(cluster, topic string) *cursor {
	q := this.queues[cluster][topic]
	return q.cursor
}

func (this *DiskService) Stop() {
	close(this.quiting)

	for _, topics := range this.queues {
		for _, q := range topics {
			q.Close()
		}
	}

	this.wg.Wait()
}

func (this *DiskService) Append(cluster, topic string, key string, value []byte) error {
	b := new(block)
	b.key = key
	b.value = value

	this.rwmux.RLock()
	q, present := this.queues[cluster][topic]
	this.rwmux.RUnlock()
	if present {
		return q.Append(b)
	}

	this.rwmux.Lock()
	defer this.rwmux.Unlock()

	// double lock check
	q, present = this.queues[cluster][topic]
	if present {
		return q.Append(b)
	}

	if err := os.Mkdir(filepath.Join(this.cfg.Dir, cluster), 0700); err != nil {
		return err
	}
	this.queues[cluster] = make(map[string]*queue)
	this.queues[cluster][topic] = newQueue(cluster, topic,
		filepath.Join(this.cfg.Dir, cluster, topic), -1)
	if err := this.queues[cluster][topic].Open(); err != nil {
		return err
	}
	return this.queues[cluster][topic].Append(b)
}

func (this *DiskService) Empty(cluster, topic string) bool {
	return false // TODO
}
