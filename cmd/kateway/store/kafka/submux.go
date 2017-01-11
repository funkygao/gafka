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

	idx map[string]int64                          // group:
	cgs map[string][]*consumergroup.ConsumerGroup // group: []cg

	lock sync.RWMutex
}

func newSubMux() *subMux {
	return &subMux{
		streams: make(map[string]*consumergroup.ConsumerGroup, 50),
		cgs:     make(map[string][]*consumergroup.ConsumerGroup, 50),
		idx:     make(map[string]int64, 50),
	}
}

func (m *subMux) get(remoteAddr string) (*consumergroup.ConsumerGroup, bool) {
	m.lock.RLock()
	cg, present := m.streams[remoteAddr]
	m.lock.RUnlock()

	return cg, present
}

func (m *subMux) claim(group, remoteAddr string) (cg *consumergroup.ConsumerGroup, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	cgsN := int64(len(m.cgs[group]))
	if cgsN == 0 {
		err = store.ErrTooManyConsumers
		return
	}

	// round robin
	cg = m.cgs[group][m.idx[group]%cgsN]

	m.streams[remoteAddr] = cg
	m.idx[group]++

	log.Debug("claim %s/%s %+v", remoteAddr, group, m)

	return
}

func (m *subMux) register(group, remoteAddr string, cg *consumergroup.ConsumerGroup) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, present := m.cgs[group]; !present {
		m.cgs[group] = make([]*consumergroup.ConsumerGroup, 0, 1)
	}

	dup := false
	for _, c := range m.cgs[group] {
		if cg.ID() != "" && cg.ID() == c.ID() {
			dup = true
			break
		}
	}

	m.streams[remoteAddr] = cg
	if !dup {
		m.cgs[group] = append(m.cgs[group], cg)
	}

	log.Debug("register %+v", m)
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
		newcgs := make([]*consumergroup.ConsumerGroup, 0)
		for _, conn := range m.cgs[cg.Name()] {
			if conn.ID() != cg.ID() {
				newcgs = append(newcgs, conn)
			}
		}
		m.cgs[cg.Name()] = newcgs
		log.Debug("safe to offload %s %+v", remoteAddr, m)
		return true
	}

	log.Debug("still %d streams left %s %+v", leftN, remoteAddr, m)

	return false
}

func (m *subMux) String() string {
	return fmt.Sprintf("mux:{streams:%+v cgs:%+v}", m.streams, m.cgs)
}
