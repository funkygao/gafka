package disk

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type queue struct {
	mu sync.RWMutex

	// Directory to create segments
	dir string

	// The head and tail segments.  Reads are from the beginning of head,
	// writes are appended to the tail.
	head, tail *segment

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return
	// an error
	maxSize int64

	// The segments that exist on disk
	segments segments
}

type queuePos struct {
	head string
	tail string
}

func newQueue(dir string, maxSize int64) (*queue, error) {
	return &queue{
		dir:            dir,
		maxSegmentSize: defaultSegmentSize,
		maxSize:        maxSize,
		segments:       segments{},
	}, nil
}

func (l *queue) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	segments, err := l.loadSegments()
	if err != nil {
		return err
	}
	l.segments = segments

	if len(l.segments) == 0 {
		_, err := l.addSegment()
		if err != nil {
			return err
		}
	}

	l.head = l.segments[0]
	l.tail = l.segments[len(l.segments)-1]

	// If the head has been fully advanced and the segment size is modified,
	// existing segments an get stuck and never allow clients to advance further.
	// This advances the segment if the current head is already at the end.
	_, err = l.head.current()
	if err == io.EOF {
		return l.trimHead()
	}

	return nil
}

func (l *queue) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	l.head = nil
	l.tail = nil
	l.segments = nil
	return nil
}

// Remove removes all underlying file-based resources for the queue.
// It is an error to call this on an open queue.
func (l *queue) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != nil || l.tail != nil || l.segments != nil {
		return fmt.Errorf("queue is open")
	}

	return os.RemoveAll(l.dir)
}

// SetMaxSegmentSize updates the max segment size for new and existing
// segments.
func (l *queue) SetMaxSegmentSize(size int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.maxSegmentSize = size

	for _, s := range l.segments {
		s.SetMaxSegmentSize(size)
	}

	if l.tail.diskUsage() >= l.maxSegmentSize {
		segment, err := l.addSegment()
		if err != nil {
			return err
		}
		l.tail = segment
	}
	return nil
}

func (l *queue) PurgeOlderThan(when time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 {
		return nil
	}

	cutoff := when.Truncate(time.Second)
	for {
		mod, err := l.head.lastModified()
		if err != nil {
			return err
		}

		if mod.After(cutoff) || mod.Equal(cutoff) {
			return nil
		}

		// If this is the last segment, first append a new one allowing
		// trimming to proceed.
		if len(l.segments) == 1 {
			_, err := l.addSegment()
			if err != nil {
				return err
			}
		}

		if err := l.trimHead(); err != nil {
			return err
		}
	}
}

// LastModified returns the last time the queue was modified.
func (l *queue) LastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.tail != nil {
		return l.tail.lastModified()
	}
	return time.Time{}.UTC(), nil
}

func (l *queue) Position() (*queuePos, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	qp := &queuePos{}
	if l.head != nil {
		qp.head = fmt.Sprintf("%s:%d", l.head.path, l.head.pos)
	}
	if l.tail != nil {
		qp.tail = fmt.Sprintf("%s:%d", l.tail.path, l.tail.filePos())
	}
	return qp, nil
}

// diskUsage returns the total size on disk used by the queue
func (l *queue) diskUsage() int64 {
	var size int64
	for _, s := range l.segments {
		size += s.diskUsage()
	}
	return size
}

// addSegment creates a new empty segment file
func (l *queue) addSegment() (*segment, error) {
	nextID, err := l.nextSegmentID()
	if err != nil {
		return nil, err
	}

	segment, err := newSegment(filepath.Join(l.dir, strconv.FormatUint(nextID, 10)), l.maxSegmentSize)
	if err != nil {
		return nil, err
	}

	l.segments = append(l.segments, segment)
	return segment, nil
}

// loadSegments loads all segments on disk
func (l *queue) loadSegments() (segments, error) {
	segments := []*segment{}

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return segments, err
	}

	for _, segment := range files {
		// Segments should be files.  Skip anything that is not a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		_, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		segment, err := newSegment(filepath.Join(l.dir, segment.Name()), l.maxSegmentSize)
		if err != nil {
			return segments, err
		}

		segments = append(segments, segment)
	}
	return segments, nil
}

// nextSegmentID returns the next segment ID that is free
func (l *queue) nextSegmentID() (uint64, error) {
	segments, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return 0, err
	}

	var maxID uint64
	for _, segment := range segments {
		// Segments should be files.  Skip anything that is not a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		segmentID, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		if segmentID > maxID {
			maxID = segmentID
		}
	}

	return maxID + 1, nil
}

// Append appends a byte slice to the end of the queue
func (l *queue) Append(b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return ErrNotOpen
	}

	if l.diskUsage()+int64(len(b)) > l.maxSize {
		return ErrQueueFull
	}

	// Append the entry to the tail, if the segment is full,
	// try to create new segment and retry the append
	if err := l.tail.append(b); err == ErrSegmentFull {
		segment, err := l.addSegment()
		if err != nil {
			return err
		}
		l.tail = segment
		return l.tail.append(b)
	}
	return nil
}

// Current returns the current byte slice at the head of the queue
func (l *queue) Current() ([]byte, error) {
	if l.head == nil {
		return nil, ErrNotOpen
	}

	return l.head.current()
}

// Advance moves the head point to the next byte slice in the queue
func (l *queue) Advance() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head == nil {
		return ErrNotOpen
	}

	err := l.head.advance()
	if err == io.EOF {
		if err := l.trimHead(); err != nil {
			return err
		}
	}

	return nil
}

func (l *queue) trimHead() error {
	if len(l.segments) > 1 {
		l.segments = l.segments[1:]

		if err := l.head.close(); err != nil {
			return err
		}
		if err := os.Remove(l.head.path); err != nil {
			return err
		}
		l.head = l.segments[0]
	}
	return nil
}
