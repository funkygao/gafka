package main

import (
	"sync"
)

type BufferPool interface {
	Get() []byte
	Put([]byte)
}

type bufferPool struct {
	p sync.Pool
}

func newBufferPool() BufferPool {
	this := &bufferPool{}
	this.p.New = func() interface{} {
		return make([]byte, 4<<10)
	}
	return this
}

func (this *bufferPool) Get() []byte {
	return this.p.Get().([]byte)
}

func (this *bufferPool) Put(b []byte) {
	this.p.Put(b)
}
