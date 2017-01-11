package kafka

import (
	"fmt"
	"sync"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/kafka-cg/consumergroup"
	log "github.com/funkygao/log4go"
)

// stream1 --+
// stream2 --+  +-- cg1 -- partition1
// stream3 --+--+
// stream4 --+  +-- cg2 -- partition2
// streamN --+
//
// stream1 --+
// stream2 --+  +-- cg3 -- partition3
// stream3 --+--+
// stream4 --+  +-- cg4 -- partition4
// streamN --+
type subMux struct {
	streams map[string]*consumergroup.ConsumerGroup // remoteAddr: cg

	idx   map[string]int64                          // group:
	stock map[string][]*consumergroup.ConsumerGroup // group: []cg

	lock sync.RWMutex
}

func newSubMux() *subMux {
	return &subMux{
		streams: make(map[string]*consumergroup.ConsumerGroup, 50),
		stock:   make(map[string][]*consumergroup.ConsumerGroup, 50),
		idx:     make(map[string]int64, 50),
	}
}

func (m *subMux) claim(group, remoteAddr string) (cg *consumergroup.ConsumerGroup, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	stockN := int64(len(m.stock[group]))
	if stockN == 0 {
		err = store.ErrTooManyConsumers
		return
	}

	// round robin
	cg = m.stock[group][m.idx[group]%stockN]

	m.streams[remoteAddr] = cg
	m.idx[group]++

	log.Debug("claim %s/%s %+v", remoteAddr, group, m)

	return
}

func (m *subMux) register(remoteAddr string, cg *consumergroup.ConsumerGroup) {
	if cg.ID() == "" {
		log.Warn("dead cg found: %+v", cg)
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	group := cg.Name()
	if _, present := m.stock[group]; !present {
		m.stock[group] = []*consumergroup.ConsumerGroup{cg}
		m.streams[remoteAddr] = cg
		log.Debug("register %s/%s %+v", remoteAddr, group, m)
		return
	}

	dup := false
	for _, c := range m.stock[group] {
		if cg.ID() == c.ID() {
			dup = true
			break
		}
	}
	if !dup {
		m.stock[group] = append(m.stock[group], cg)
	}

	m.streams[remoteAddr] = cg

	log.Debug("register %s/%s %+v", remoteAddr, group, m)
}

func (m *subMux) kill(remoteAddr string) bool {
	m.lock.RLock()
	cg, found := m.streams[remoteAddr]
	m.lock.RUnlock()
	if !found {
		return true
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	// unbind the stream from cg
	delete(m.streams, remoteAddr)

	leftN := 0
	for _, s := range m.streams {
		if s.ID() == cg.ID() {
			leftN++
		}
	}

	if leftN == 0 {
		newstock := make([]*consumergroup.ConsumerGroup, 0)
		for _, conn := range m.stock[cg.Name()] {
			if conn.ID() != cg.ID() {
				newstock = append(newstock, conn)
			}
		}
		m.stock[cg.Name()] = newstock
		log.Debug("safe to offload %s %+v", remoteAddr, m)
		return true
	}

	log.Debug("still %d streams left %s %+v", leftN, remoteAddr, m)

	return false
}

func (m *subMux) String() string {
	return fmt.Sprintf("mux:{streams:%+v stock:%+v}", m.streams, m.stock)
}
