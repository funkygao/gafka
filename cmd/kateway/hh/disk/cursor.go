package disk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type position struct {
	Offset    int64
	SegmentID uint64
}

type cursor struct {
	ctx *queue

	seg *segment

	rwmux sync.RWMutex
	pos   position
	dirty bool
}

func newCursor(q *queue) *cursor {
	return &cursor{
		ctx: q,
	}
}

// open loads latest cursor position from disk
func (c *cursor) open() error {
	f, err := os.OpenFile(c.cursorFile(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	if err = dec.Decode(&c.pos); err != nil {
		// the cursor file has just been created with empty contents
		c.moveToHead()
	} else if c.pos.SegmentID < c.ctx.head.id {
		c.moveToHead()
	}

	s, present := c.findSegment(c.pos.SegmentID)
	if !present {
		return ErrCursorNotFound
	}

	c.seg = s
	return s.Seek(c.pos.Offset)
}

func (c *cursor) findSegment(id uint64) (*segment, bool) {
	for _, s := range c.ctx.segments {
		if s.id == id {
			return s, true
		}
	}

	return nil, false
}

func (c *cursor) cursorFile() string {
	return filepath.Join(c.ctx.dir, cursorFile)
}

// dump save the cursor position to disk.
// housekeeping will periodically checkpoint with dump.
func (c *cursor) dump() error {
	c.rwmux.Lock()
	defer c.rwmux.Unlock()

	if !c.dirty {
		return nil
	}

	f, err := os.OpenFile(c.cursorFile(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if err = enc.Encode(&c.pos); err != nil {
		return err
	}

	c.dirty = false

	return nil
}

func (c *cursor) moveToHead() {
	c.pos.Offset = 0
	c.pos.SegmentID = c.ctx.head.id
	c.dirty = true
}

func (c *cursor) advanceOffset(delta int64) (err error) {
	c.rwmux.Lock()
	if c.pos.Offset+delta < 0 {
		c.rwmux.Unlock()
		return ErrCursorOutOfRange
	}

	c.pos.Offset += delta
	c.dirty = true
	c.rwmux.Unlock()
	return
}

func (c *cursor) advanceSegment() (ok bool) {
	c.rwmux.Lock()
	defer c.rwmux.Unlock()

	for _, seg := range c.ctx.segments {
		if seg.id > c.pos.SegmentID {
			c.pos.SegmentID = seg.id
			c.seg = seg
			c.pos.Offset = 0
			c.seg.Seek(0)
			c.dirty = true
			return true
		}
	}

	// tail reached
	return false
}
