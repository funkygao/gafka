package disk

import (
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
)

// Segment is a queue using a single file.  The structure of a segment is a series
// of TLV block.
//
// ┌───────────────────────────────────────────────────────────┐ ┌───────────────────────────────────────────────────────────┐
// |                    Block 1                                | |                    Block 2                                |
// └───────────────────────────────────────────────────────────┘ └───────────────────────────────────────────────────────────┘
// ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───────────┐ ┌─────────┐
// | magic   | | key len | | key     | | value len | | value   | | magic   | | key len | | key     | | value len | | value   |
// | 2 bytes | | 4 bytes | | N bytes | | 4 bytes   | | N bytes | | 2 bytes | | 4 bytes | | N bytes | | 4 bytes   | | N bytes |
// └─────────┘ └─────────┘ └─────────┘ └───────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └───────────┘ └─────────┘
//
// Segments store arbitrary byte slices and leave the serialization to the caller.  Segments
// are created with a max size and will block writes when the segment is full.
type segment struct {
	mu sync.RWMutex

	id      uint64
	size    int64
	maxSize int64

	rfile *bufferReader
	wfile *bufferWriter

	lastFlush      time.Time
	flushInflights int

	buf []byte // reuseable buf to read blocks
}

type segments []*segment

func newSegment(id uint64, path string, maxSize int64) (*segment, error) {
	// TODO should explicitly open files: too many open files?
	wf, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	rf, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}

	stats, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &segment{
		id:      id,
		wfile:   newBufferWriter(wf),
		rfile:   newBufferReader(rf),
		size:    stats.Size(),
		maxSize: maxSize,
	}, nil
}

func (s *segment) Append(b *block) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wfile == nil {
		return ErrSegmentNotOpen
	}

	if s.size+b.size() > s.maxSize {
		return ErrSegmentFull
	}

	if err = b.writeTo(s.wfile); err != nil {
		return
	}

	if err := s.flush(); err != nil {
		return err
	}

	s.size += b.size()

	return nil
}

func (s *segment) ReadOne(b *block) error {
	if s.rfile == nil {
		return ErrSegmentNotOpen
	}

	if len(s.buf) == 0 {
		s.buf = make([]byte, maxBlockSize)
	}

	if err := b.readFrom(s.rfile, s.buf); err != nil {
		return err
	}

	return nil
}

func (s *segment) flush() (err error) {
	if s.wfile == nil {
		return ErrSegmentNotOpen
	}

	if s.lastFlush.IsZero() {
		// the 1st flush always do real IO
		if err = s.wfile.Sync(); err == nil {
			s.lastFlush = time.Now()
		}
		return
	}

	now := time.Now()
	if s.flushInflights >= flushEveryBlocks || now.Sub(s.lastFlush) >= flushInterval {
		// time to flush the batch
		if err = s.wfile.Sync(); err == nil {
			s.flushInflights = 0
			s.lastFlush = now
		}
	} else {
		// batch it up to avoid real IO
		s.flushInflights++
	}

	return
}

func (s *segment) Current() int64 {
	if s.rfile == nil {
		return -1
	}

	n, _ := s.rfile.Seek(0, os.SEEK_CUR)
	return n
}

func (s *segment) Remove() (err error) {
	if s.wfile == nil {
		return ErrSegmentNotOpen
	}

	path := s.wfile.Name()
	log.Trace("segment[%s] removed", path)

	if err = s.Close(); err != nil {
		return
	}
	if err = os.Remove(path); err != nil {
		return
	}

	return
}

func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wfile.Close(); err != nil {
		return err
	}
	if err := s.rfile.Close(); err != nil {
		return err
	}
	s.wfile = nil
	s.rfile = nil
	return nil
}

func (s *segment) LastModified() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats, _ := os.Stat(s.wfile.Name())
	return stats.ModTime().UTC()
}

func (s *segment) DiskUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *segment) Seek(pos int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rfile == nil {
		return ErrSegmentNotOpen
	}

	n, err := s.rfile.Seek(pos, os.SEEK_SET)
	if err != nil {
		return err
	}

	if n != pos {
		return fmt.Errorf("bad seek. exp %v, got %v", pos, n)
	}

	return nil
}
