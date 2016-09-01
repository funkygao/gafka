package disk

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	//log "github.com/funkygao/log4go"
)

type position struct {
	Offset    int64
	SegmentId uint64
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

func (c *cursor) cursorFile() string {
	return filepath.Join(c.ctx.dir, cursorFile)
}

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

// goroutine unsafe
func (c *cursor) resetPosition() {
	c.pos.Offset = 0
	c.pos.SegmentId = c.ctx.head.id
}

func (c *cursor) open() error {
	f, err := os.OpenFile(c.cursorFile(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	if err = dec.Decode(&c.pos); err != nil {
		// the cursor file has just been created with empty contents
		c.resetPosition()
	} else if c.pos.SegmentId < c.ctx.head.id {
		// the outdated segment has been purged
		c.resetPosition()
	}

	s, present := c.ctx.SegmentById(c.pos.SegmentId)
	if !present {
		c.resetPosition()
		s, present = c.ctx.SegmentById(c.pos.SegmentId)
	}

	if !present {
		return ErrCursorNotFound
	}

	c.seg = s
	return s.Seek(c.pos.Offset)
}

func (c *cursor) Next(b *block) (err error) {
	err = c.seg.ReadOne(b)
	if err == nil {
		c.pos.Offset = c.seg.Current()
	}
	if err == io.EOF {
		for {
			ok := c.advance()
			err = c.seg.ReadOne(b)
			if err == nil {
				c.pos.Offset = c.seg.Current()
				return
			}

			// err occurs

			if !ok {
				// tail reached
				return
			}
		}
	}

	return
}

func (c *cursor) advance() bool {
	for _, seg := range c.ctx.segments {
		if seg.id > c.pos.SegmentId {
			c.pos.SegmentId = seg.id
			c.seg = seg
			c.pos.Offset = 0
			c.seg.Seek(c.pos.Offset)
			return true
		}
	}

	// tail reached
	return false
}
