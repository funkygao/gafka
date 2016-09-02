package disk

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	log "github.com/funkygao/log4go"
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
}

func newCursor(q *queue) *cursor {
	return &cursor{
		ctx: q,
	}
}

// open loads latest cursor position from disk
func (c *cursor) open() error {
	log.Debug("cursor[%s] open...", c.cursorFile())

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
	log.Debug("cursor[%s] dump...", c.cursorFile())

	f, err := os.OpenFile(c.cursorFile(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	c.rwmux.RLock()
	defer c.rwmux.RUnlock()

	enc := json.NewEncoder(f)
	if err = enc.Encode(&c.pos); err != nil {
		return err
	}

	return nil
}

func (c *cursor) moveToHead() {
	c.pos.Offset = 0
	c.pos.SegmentID = c.ctx.head.id
}

func (c *cursor) advanceOffset(delta int64) {
	c.rwmux.Lock()
	c.pos.Offset += delta
	c.rwmux.Unlock()
}

func (c *cursor) advanceSegment() (ok bool) {
	for _, seg := range c.ctx.segments {
		if seg.id > c.pos.SegmentID {
			c.pos.SegmentID = seg.id
			c.seg = seg
			c.pos.Offset = 0
			c.seg.Seek(0)
			return true
		}
	}

	// tail reached
	return false
}
