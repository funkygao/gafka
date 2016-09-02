package disk

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type position struct {
	Offset    int64
	SegmentID uint64
}

type cursor struct {
	ctx *queue

	seg *segment
	pos position
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
	f, err := os.OpenFile(c.cursorFile(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

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

func (c *cursor) advance() (ok bool) {
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
