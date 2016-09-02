package disk

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// queue is a bounded, disk-backed, append-only type that combines queue and
// log semantics.
// key/value byte slices can be appended and read back in order through
// cursor.
//
// Internally, the queue writes key/value byte slices to multiple segment files so
// that disk space can be reclaimed. When a segment file is larger than
// the max segment size, a new file is created.   Segments are removed
// after cursor has advanced past the last entry.  The first
// segment is the head, and the last segment is the tail.  Reads are from
// the head segment and writes tail segment.
//
// queues can have a max size configured such that when the size of all
// segments on disk exceeds the size, write will fail.
//
// ┌─────┐
// │Head │
// ├─────┘
// │
// ▼
// ┌─────────────────┐ ┌─────────────────┐┌─────────────────┐
// │Segment 1 - 10MB │ │Segment 2 - 10MB ││Segment 3 - 10MB │
// └─────────────────┘ └─────────────────┘└─────────────────┘
//                          ▲                               ▲
//                          |                               │
//                          |                               │
//                        cursor                       ┌─────┐
//                                                     │Tail │
//                                                     └─────┘
type queue struct {
	mu sync.RWMutex

	// Directory to create segments
	dir           string
	clusterTopic  clusterTopic
	quit          chan struct{}
	emptyInflight bool

	// The head and tail segments.  Reads are from the beginning of head,
	// writes are appended to the tail.
	head, tail *segment

	cursor *cursor

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return an error
	maxSize int64

	// The segments that exist on disk
	segments segments
}

// newQueue create a queue that will store segments in dir and that will
// consume more than maxSize on disk.
func newQueue(ct clusterTopic, dir string, maxSize int64) *queue {
	q := &queue{
		clusterTopic:   ct,
		dir:            dir,
		quit:           make(chan struct{}),
		maxSegmentSize: defaultSegmentSize,
		maxSize:        maxSize,
		segments:       segments{},
	}
	q.cursor = newCursor(q)
	return q
}

// Open opens the queue for reading and writing
func (l *queue) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := mkdirIfNotExist(l.dir); err != nil {
		return err
	}

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

	// cursor open must be placed below queue open
	if err = l.cursor.open(); err != nil {
		return err
	}

	return nil
}

// Close stops the queue for reading and writing
func (l *queue) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	l.head = nil
	l.tail = nil
	l.segments = nil
	close(l.quit)
	return l.cursor.dump()
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

func (l *queue) Purge() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) <= 1 {
		// head, curror, tail are in the same segment
		return nil
	}

	for {
		// If this is the last segment, first append a new one allowing
		// trimming to proceed.
		if len(l.segments) == 1 {
			if _, err := l.addSegment(); err != nil {
				return err
			}
		}

		if l.cursor.pos.SegmentId > l.head.id {
			l.trimHead()
		} else {
			return nil
		}

	}
}

func (l *queue) trimHead() (err error) {
	l.segments = l.segments[1:]

	if err = l.head.Remove(); err != nil {
		return
	}

	l.head = l.segments[0]

	return
}

func (l *queue) nextDir() string {
	// find least loaded dir
	return ""
}

// LastModified returns the last time the queue was modified.
func (l *queue) LastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.tail != nil {
		return l.tail.LastModified()
	}
	return time.Time{}.UTC(), nil
}

func (l *queue) SegmentById(id uint64) (*segment, bool) {
	for _, s := range l.segments {
		if s.id == id {
			return s, true
		}
	}

	return nil, false
}

// diskUsage returns the total size on disk used by the queue
func (l *queue) diskUsage() int64 {
	var size int64
	for _, s := range l.segments {
		size += s.DiskUsage()
	}
	return size
}

// addSegment creates a new empty segment file
func (l *queue) addSegment() (*segment, error) {
	nextID, err := l.nextSegmentID()
	if err != nil {
		return nil, err
	}

	segment, err := newSegment(nextID, filepath.Join(l.dir, strconv.FormatUint(nextID, 10)), l.maxSegmentSize)
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
		if segment.IsDir() || segment.Name() == cursorFile {
			continue
		}

		// Segments file names are all numeric
		id, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		segment, err := newSegment(id, filepath.Join(l.dir, segment.Name()), l.maxSegmentSize)
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
func (l *queue) Append(b *block) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return ErrNotOpen
	}

	if l.maxSize > 0 && l.diskUsage()+b.size() > l.maxSize {
		return ErrQueueFull
	}

	// Append the entry to the tail, if the segment is full,
	// try to create new segment and retry the append
	if err := l.tail.Append(b); err == ErrSegmentFull {
		segment, err := l.addSegment()
		if err != nil {
			return err
		}
		l.tail = segment
		return l.tail.Append(b)
	} else if err != nil {
		return err
	}
	return nil
}

func (l *queue) EmptyInflight() bool {
	return l.emptyInflight
}
