package mpool

import (
	"errors"

	log "github.com/funkygao/log4go"
)

var ErrorMessageOverflow = errors.New("message overflow")

// Message encapsulates the messages that we exchange back and forth.
type Message struct {
	Body    []byte
	bodyBuf []byte

	slabSize int
}

type slabClass struct {
	maxSize int
	ch      chan *Message
}

// round n up to a multiple of a.  a must be a power of 2.
func round(n, a int) int {
	return (n + a - 1) &^ (a - 1)
}

func isPowerOfTwo(n int) bool {
	return (n & (n - 1)) == 0
}

var messagePool = []slabClass{
	{maxSize: 256, ch: make(chan *Message, 20<<10)},     // 5MB   = 256 * 20K
	{maxSize: 1024, ch: make(chan *Message, 50<<10)},    // 50MB  = 1K * 50K
	{maxSize: 2 << 10, ch: make(chan *Message, 50<<10)}, // 100MB = 2K * 50K
	{maxSize: 4 << 10, ch: make(chan *Message, 50<<10)}, // 200MB = 4K * 50K
	{maxSize: 8 << 10, ch: make(chan *Message, 4<<10)},  // 32MB  = 8K * 4K
	{maxSize: 64 << 10, ch: make(chan *Message, 1<<10)}, // 64MB  = 64K * 1K
	{maxSize: 256 << 10, ch: make(chan *Message, 1<<7)}, // 32MB  = 256K * 128
}

// NewMessage is the supported way to obtain a new Message.  This makes
// use of a "slab allocator" which greatly reduces the load on the
// garbage collector.
func NewMessage(size int) *Message {
	var ch chan *Message
	for _, slabClass := range messagePool { // TODO binary search
		if size <= slabClass.maxSize {
			ch = slabClass.ch
			size = slabClass.maxSize
			break
		}
	}

	var msg *Message
	select {
	case msg = <-ch:
	default:
		// message pool empty:
		// too busy or size greater than largest slab class
		log.Trace("allocating message memory pool: %dB", size)

		msg = &Message{}
		msg.slabSize = size
		msg.bodyBuf = make([]byte, 0, msg.slabSize)
	}

	msg.Body = msg.bodyBuf
	return msg
}

// Free decrements the reference count on a message, and releases its
// resources if no further references remain.  While this is not
// strictly necessary thanks to GC, doing so allows for the resources to
// be recycled without engaging GC.  This can have rather substantial
// benefits for performance.
func (this *Message) Free() (recycled bool) {
	var ch chan *Message
	for _, slab := range messagePool {
		if this.slabSize == slab.maxSize {
			ch = slab.ch
			break
		}
	}

	select {
	case ch <- this:
	default:
		// e,g. channel size 10, NewMessage alloc 20 messages while nobody free's
		// then 1-10 free ok, but 11-20 will trigger this branch
		log.Warn("slab class[%d] full %d, fallback to GC", this.slabSize, len(ch))
		this.bodyBuf = nil
		//this = nil TODO
	}

	return true
}
