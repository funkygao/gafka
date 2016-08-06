package mpool

import (
	"sync"
)

type Intern struct {
	sync.RWMutex
	lookup map[string]string
}

func NewIntern() *Intern {
	return &Intern{lookup: make(map[string]string, 10)}
}

func (this *Intern) String(s string) string {
	this.RLock()
	ss, present := this.lookup[s]
	this.RUnlock()
	if present {
		return ss
	}

	this.Lock()
	ss, present = this.lookup[s]
	if present {
		this.Unlock()
		return ss
	}

	this.lookup[s] = s
	this.Unlock()
	return s
}
