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
	offset   int
}

type slabClass struct {
	maxSize int
	ch      chan *Message
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
		// this slab class pool is full, silently drop
	}

	return true
}

func (this *Message) Reset() {
	this.offset = 0
}

// WriteString is a costly function. Use it only when you know its underhood.
// Currently, kateway is not using this function.
func (this *Message) WriteString(s string) error {
	if len(s)+this.offset > cap(this.bodyBuf) {
		return ErrorMessageOverflow
	}

	this.Body = this.Body[0 : this.offset+len(s)]
	copy(this.Body[this.offset:], s)
	this.offset += len(s)
	return nil
}

func (this *Message) Write(b []byte) error {
	if len(b)+this.offset > cap(this.bodyBuf) {
		return ErrorMessageOverflow
	}

	this.Body = this.Body[0 : this.offset+len(b)]
	copy(this.Body[this.offset:], b)
	this.offset += len(b)
	return nil
}

func (this *Message) Bytes() []byte {
	return this.Body[0:]
}

// NewMessage is the supported way to obtain a new Message.  This makes
// use of a "slab allocator" which greatly reduces the load on the
// garbage collector.
func NewMessage(size int) *Message {
	var msg *Message
	var ch chan *Message
	for _, slabClass := range messagePool { // TODO binary search
		if size <= slabClass.maxSize {
			ch = slabClass.ch
			size = slabClass.maxSize
			break
		}
	}

	select {
	case msg = <-ch:
	default:
		// message pool empty:
		// too busy or size greater than largest slab class
		msg = &Message{}
		msg.slabSize = size
		msg.bodyBuf = make([]byte, 0, msg.slabSize)
	}

	msg.Body = msg.bodyBuf
	return msg
}
