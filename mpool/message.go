package mpool

import (
	"sync/atomic"
)

// Message encapsulates the messages that we exchange back and forth.
type Message struct {
	Body    []byte
	bodyBuf []byte

	slabSize int
	refCount int32
}

type messageSlab struct {
	maxBody int
	ch      chan *Message
}

var messagePool = []messageSlab{
	{maxBody: 256, ch: make(chan *Message, 1024)},  // 128K
	{maxBody: 1024, ch: make(chan *Message, 1024)}, // 1 MB
	{maxBody: 8192, ch: make(chan *Message, 256)},  // 2 MB
	{maxBody: 65536, ch: make(chan *Message, 64)},  // 4 MB
}

// Free decrements the reference count on a message, and releases its
// resources if no further references remain.  While this is not
// strictly necessary thanks to GC, doing so allows for the resources to
// be recycled without engaging GC.  This can have rather substantial
// benefits for performance.
func (this *Message) Free() (recycled bool) {
	refCount := atomic.AddInt32(&this.refCount, -1)
	if refCount > 0 {
		return false
	} else if refCount < 0 {
		// should never happen
		return true
	}

	// safe to put back message pool for later reuse
	var ch chan *Message
	for _, slab := range messagePool {
		if this.slabSize == slab.maxBody {
			ch = slab.ch
			break
		}
	}

	select {
	case ch <- this:
	default:
		// message pool is full, silently drop
	}
	return true
}

func (this *Message) Clone() *Message {
	atomic.AddInt32(&this.refCount, 1)
	return this
}

// NewMessage is the supported way to obtain a new Message.  This makes
// use of a "slab allocator" which greatly reduces the load on the
// garbage collector.
func NewMessage(sz int) *Message {
	var msg *Message
	var ch chan *Message
	for _, slab := range messagePool {
		if sz <= slab.maxBody {
			ch = slab.ch
			sz = slab.maxBody
			break
		}
	}

	select {
	case msg = <-ch:
	default:
		// message pool empty
		msg = &Message{}
		msg.slabSize = sz
		msg.bodyBuf = make([]byte, 0, msg.slabSize)
	}

	msg.refCount = 1
	msg.Body = msg.bodyBuf
	return msg
}
