package disk

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type Service struct {
	cfg *Config

	quiting chan struct{}
	wg      sync.WaitGroup

	rwmux sync.RWMutex

	// hh
	// ├── cluster1
	// └── cluster2
	//     ├── topic1
	//     └── topic2
	//         ├── 1
	//         ├── 2
	//         ├── 3
	//         └── cursor.dmp
	queues map[clusterTopic]*queue
}

func New(cfg *Config) *Service {
	return &Service{
		cfg:     cfg,
		quiting: make(chan struct{}),
		queues:  make(map[clusterTopic]*queue),
	}
}

func (this *Service) Start() (err error) {
	if err = mkdirIfNotExist(this.cfg.Dir); err != nil {
		return
	}

	return this.loadQueues(this.cfg.Dir)
}

func (this *Service) Stop() {
	close(this.quiting)

	for _, q := range this.queues {
		q.Close()
	}

	this.wg.Wait()
}

func (this *Service) Append(cluster, topic string, key, value []byte) error {
	b := &block{key: key, value: value}
	ct := clusterTopic{cluster: cluster, topic: topic}

	this.rwmux.RLock()
	q, present := this.queues[ct]
	this.rwmux.RUnlock()
	if present {
		return q.Append(b)
	}

	this.rwmux.Lock()
	defer this.rwmux.Unlock()

	// double lock check
	q, present = this.queues[ct]
	if present {
		return q.Append(b)
	}

	if err := this.createAndStartQueue(ct); err != nil {
		return err
	}

	return this.queues[ct].Append(b)
}

// TODO
func (this *Service) Empty(cluster, topic string) bool {
	ct := clusterTopic{cluster: cluster, topic: topic}

	this.rwmux.RLock()
	q, present := this.queues[ct]
	this.rwmux.RUnlock()

	if !present {
		// should never happen
		return true
	}

	return q.EmptyInflight()
}

func (this *Service) loadQueues(dir string) error {
	clusters, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	// load queues from disk
	for _, cluster := range clusters {
		if !cluster.IsDir() {
			continue
		}

		topics, err := ioutil.ReadDir(filepath.Join(dir, cluster.Name()))
		if err != nil {
			return err
		}

		for _, topic := range topics {
			if !topic.IsDir() {
				continue
			}

			ct := clusterTopic{cluster: cluster.Name(), topic: topic.Name()}
			if err = this.createAndStartQueue(ct); err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *Service) createAndStartQueue(ct clusterTopic) error {
	if err := os.Mkdir(ct.ClusterDir(this.cfg.Dir), 0700); err != nil && !os.IsExist(err) {
		return err
	}

	this.queues[ct] = newQueue(ct, ct.TopicDir(this.cfg.Dir), -1)
	if err := this.queues[ct].Open(); err != nil {
		return err
	}

	this.wg.Add(1)
	go this.queues[ct].housekeeping(this.cfg.PurgeInterval, &this.wg)
	return nil
}
