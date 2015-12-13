package mpool

import (
	"errors"
)

var ErrorMessageOverflow = errors.New("message overflow")

// Message encapsulates the messages that we exchange back and forth.
type Message struct {
	Body    []byte
	bodyBuf []byte

	slabSize int
	off      int
}

type messageSlab struct {
	maxBody int
	ch      chan *Message
}

var messagePool = []messageSlab{
	{maxBody: 256, ch: make(chan *Message, 1<<10)},      // 256K
	{maxBody: 2 << 10, ch: make(chan *Message, 20<<10)}, // 40 MB
	{maxBody: 8 << 10, ch: make(chan *Message, 1<<10)},  // 8 MB
	{maxBody: 64 << 10, ch: make(chan *Message, 1<<8)},  // 16 MB
}

// Free decrements the reference count on a message, and releases its
// resources if no further references remain.  While this is not
// strictly necessary thanks to GC, doing so allows for the resources to
// be recycled without engaging GC.  This can have rather substantial
// benefits for performance.
func (this *Message) Free() (recycled bool) {
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

func (this *Message) Reset() {
	this.off = 0
}

func (this *Message) WriteString(s string) error {
	if len(s)+this.off > cap(this.bodyBuf) {
		return ErrorMessageOverflow
	}

	this.Body = this.Body[0 : this.off+len(s)]
	copy(this.Body[this.off:], s)
	this.off += len(s)
	return nil
}

func (this *Message) Bytes() []byte {
	return this.Body[0:]
}

func (this *Message) String() string {
	return string(this.Body)
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

	msg.Body = msg.bodyBuf
	return msg
}
