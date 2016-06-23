package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type segments []*segment

type segment struct {
	mu sync.RWMutex

	size int64
	file *os.File
	path string

	pos         int64
	currentSize int64
	maxSize     int64
}

func newSegment(path string, maxSize int64) (*segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	stats, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	s := &segment{file: f, path: path, size: stats.Size(), maxSize: maxSize}

	if err := s.open(); err != nil {
		return nil, err
	}

	return s, nil
}

func (l *segment) open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If it's a new segment then write the location of the current record in this segment
	if l.size == 0 {
		l.pos = 0
		l.currentSize = 0

		if err := l.writeUint64(uint64(l.pos)); err != nil {
			return err
		}

		if err := l.file.Sync(); err != nil {
			return err
		}

		l.size = footerSize

		return nil
	}

	// Existing segment so read the current position and the size of the current block
	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	pos, err := l.readUint64()
	if err != nil {
		return err
	}
	l.pos = int64(pos)

	if err := l.seekToCurrent(); err != nil {
		return err
	}

	// If we're at the end of the segment, don't read the current block size,
	// it's 0.
	if l.pos < l.size-footerSize {
		currentSize, err := l.readUint64()
		if err != nil {
			return err
		}
		l.currentSize = int64(currentSize)
	}

	return nil
}

// append adds byte slice to the end of segment
func (l *segment) append(b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrNotOpen
	}

	if l.size+int64(len(b)) > l.maxSize {
		return ErrSegmentFull
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	if err := l.writeUint64(uint64(len(b))); err != nil {
		return err
	}

	if err := l.writeUint64(uint64(len(b))); err != nil {
		return err
	}

	if err := l.writeBytes(b); err != nil {
		return err
	}

	if err := l.writeUint64(uint64(l.pos)); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}

	if l.currentSize == 0 {
		l.currentSize = int64(len(b))
	}

	l.size += int64(len(b)) + 8 // uint64 for slice length

	return nil
}

// current returns byte slice that the current segment points
func (l *segment) current() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if int64(l.pos) == l.size-8 {
		return nil, io.EOF
	}

	if err := l.seekToCurrent(); err != nil {
		return nil, err
	}

	// read the record size
	sz, err := l.readUint64()
	if err != nil {
		return nil, err
	}
	l.currentSize = int64(sz)

	if int64(sz) > l.maxSize {
		return nil, fmt.Errorf("record size out of range: max %d: got %d", l.maxSize, sz)
	}

	b := make([]byte, sz)
	if err := l.readBytes(b); err != nil {
		return nil, err
	}

	return b, nil
}

// advance advances the current value pointer
func (l *segment) advance() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrNotOpen
	}

	// If we're at the end of the file, can't advance
	if int64(l.pos) == l.size-footerSize {
		l.currentSize = 0
		return io.EOF
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	pos := l.pos + l.currentSize + 8
	if err := l.writeUint64(uint64(pos)); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}
	l.pos = pos

	if err := l.seekToCurrent(); err != nil {
		return err
	}

	sz, err := l.readUint64()
	if err != nil {
		return err
	}
	l.currentSize = int64(sz)

	if int64(l.pos) == l.size-footerSize {
		l.currentSize = 0
		return io.EOF
	}

	return nil
}

func (l *segment) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Close(); err != nil {
		return err
	}
	l.file = nil
	return nil
}

func (l *segment) lastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats, err := os.Stat(l.file.Name())
	if err != nil {
		return time.Time{}, err
	}
	return stats.ModTime().UTC(), nil
}

func (l *segment) diskUsage() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

func (l *segment) SetMaxSegmentSize(size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxSize = size
}

func (l *segment) seekToCurrent() error {
	return l.seek(int64(l.pos))
}

func (l *segment) seek(pos int64) error {
	n, err := l.file.Seek(pos, os.SEEK_SET)
	if err != nil {
		return err
	}

	if n != pos {
		return fmt.Errorf("bad seek. exp %v, got %v", 0, n)
	}

	return nil
}
func (l *segment) seekEnd(pos int64) error {
	_, err := l.file.Seek(pos, os.SEEK_END)
	if err != nil {
		return err
	}

	return nil
}

func (l *segment) filePos() int64 {
	n, _ := l.file.Seek(0, os.SEEK_CUR)
	return n
}

func (l *segment) readUint64() (uint64, error) {
	b := make([]byte, 8)
	if err := l.readBytes(b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (l *segment) writeUint64(sz uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sz)
	return l.writeBytes(buf[:])
}

func (l *segment) writeBytes(b []byte) error {
	n, err := l.file.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("short write. got %d, exp %d", n, len(b))
	}
	return nil
}

func (l *segment) readBytes(b []byte) error {
	n, err := l.file.Read(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("bad read. exp %v, got %v", 0, n)
	}
	return nil
}
