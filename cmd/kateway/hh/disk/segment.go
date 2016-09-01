package disk

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

// Segment is a queue using a single file.  The structure of a segment is a series
// of TLV block.
// TODO add crc32 checksum
//
// ┌───────────────────────────────────────────────┐ ┌───────────────────────────────────────────────┐
// |                    Block 1                    | |                    Block 2                    |
// └───────────────────────────────────────────────┘ └───────────────────────────────────────────────┘
// ┌─────────┐ ┌─────────┐ ┌───────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌───────────┐ ┌─────────┐
// | key len | | key     | | value len | | value   | | key len | | key     | | value len | | value   |
// | 4 bytes | | N bytes | | 4 bytes   | | N bytes | | 4 bytes | | N bytes | | 4 bytes   | | N bytes |
// └─────────┘ └─────────┘ └───────────┘ └─────────┘ └─────────┘ └─────────┘ └───────────┘ └─────────┘
//
// Segments store arbitrary byte slices and leave the serialization to the caller.  Segments
// are created with a max size and will block writes when the segment is full.
type segment struct {
	mu sync.RWMutex

	id      uint64
	size    int64
	maxSize int64

	wfile, rfile *os.File

	rbuf, wbuf [4]byte
	buf        []byte
}

type segments []*segment

func newSegment(id uint64, path string, maxSize int64) (*segment, error) {
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
		wfile:   wf,
		rfile:   rf,
		size:    stats.Size(),
		maxSize: maxSize,
	}, nil
}

func (s *segment) Append(b *block) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wfile == nil {
		return ErrNotOpen
	}

	if s.size+b.size() > s.maxSize {
		return ErrSegmentFull
	}

	if err = s.writeUint32(b.keyLen()); err != nil {
		return
	}

	// FIXME what if fails here? crc32
	if err := s.writeBytes([]byte(b.key)); err != nil {
		return err
	}

	if err = s.writeUint32(b.valueLen()); err != nil {
		return
	}

	if err = s.writeBytes(b.value); err != nil {
		return
	}

	// TODO
	if err := s.Flush(); err != nil {
		return err
	}

	s.size += b.size()

	return nil
}

func (s *segment) ReadOne(b *block) error {
	if s.rfile == nil {
		return ErrNotOpen
	}

	keyLen, err := s.readUint32()
	if err != nil {
		return err
	}

	if len(s.buf) == 0 {
		s.buf = make([]byte, 1<<20)
	}

	if err = s.readBytes(s.buf[:int(keyLen)]); err != nil {
		return err
	}
	b.key = string(s.buf[:int(keyLen)])

	valueLen, err := s.readUint32()
	if err != nil {
		return err
	}

	if err = s.readBytes(s.buf[:int(valueLen)]); err != nil {
		return err
	}
	b.value = s.buf[:int(valueLen)]
	return nil
}

func (s *segment) Flush() error {
	return s.wfile.Sync()
}

func (s *segment) Current() int64 {
	n, _ := s.rfile.Seek(0, os.SEEK_CUR)
	return n
}

func (s *segment) Remove() (err error) {
	if err = s.Close(); err != nil {
		return
	}
	if err = os.Remove(s.wfile.Name()); err != nil {
		return
	}

	return
}

func (l *segment) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.wfile.Close(); err != nil {
		return err
	}
	if err := l.rfile.Close(); err != nil {
		return err
	}
	l.wfile = nil
	l.rfile = nil
	return nil
}

func (s *segment) LastModified() (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats, err := os.Stat(s.wfile.Name())
	if err != nil {
		return time.Time{}, err
	}
	return stats.ModTime().UTC(), nil
}

func (s *segment) DiskUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *segment) Seek(pos int64) error {
	n, err := s.rfile.Seek(pos, os.SEEK_SET)
	if err != nil {
		return err
	}

	if n != pos {
		return fmt.Errorf("bad seek. exp %v, got %v", 0, n)
	}

	return nil
}

func (s *segment) readUint32() (uint32, error) {
	if err := s.readBytes(s.rbuf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(s.rbuf[:]), nil
}

func (s *segment) writeUint32(v uint32) error {
	binary.BigEndian.PutUint32(s.wbuf[:], v)
	return s.writeBytes(s.wbuf[:])
}

func (s *segment) writeBytes(b []byte) error {
	n, err := s.wfile.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("short write. got %d, exp %d", n, len(b))
	}
	return nil
}

func (s *segment) readBytes(b []byte) error {
	n, err := s.rfile.Read(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("bad read. exp %v, got %v", 0, n)
	}
	return nil
}
