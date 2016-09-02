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

	log "github.com/funkygao/log4go"
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
// │head │
// ├─────┘
// │
// ▼
// ┌─────────────────┐ ┌─────────────────┐┌─────────────────┐
// │segment 1 - 10MB │ │segment 2 - 10MB ││segment 3 - 10MB │
// └─────────────────┘ └─────────────────┘└─────────────────┘
//                          ▲                               ▲
//                          │                               │
//                          │                               │
//                       ┌───────┐                     ┌─────┐
//                       │cursor │                     │tail │
//                       └───────┘                     └─────┘
type queue struct {
	mu sync.RWMutex
	wg sync.WaitGroup

	dir          string // Directory to create segments
	clusterTopic clusterTopic

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return an error
	// -1 means unlimited
	maxSize int64

	purgeInterval time.Duration

	cursor     *cursor
	head, tail *segment
	segments   segments

	quit          chan struct{}
	emptyInflight bool // FIXME
}

// newQueue create a queue that will store segments in dir and that will
// consume more than maxSize on disk.
func newQueue(ct clusterTopic, dir string, maxSize int64, purgeInterval time.Duration) *queue {
	q := &queue{
		clusterTopic:   ct,
		dir:            dir,
		quit:           make(chan struct{}),
		maxSegmentSize: defaultSegmentSize,
		maxSize:        maxSize,
		purgeInterval:  purgeInterval,
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
		// create the 1st segment
		if _, err = l.addSegment(); err != nil {
			return err
		}
	}

	l.head = l.segments[0]
	l.tail = l.segments[len(l.segments)-1]

	// cursor open must be placed below queue open
	if err = l.cursor.open(); err != nil {
		return err
	}

	l.wg.Add(1)
	go l.housekeeping()

	l.wg.Add(1)
	go l.pump()

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

	l.wg.Wait()
	if err := l.cursor.dump(); err != nil {
		return err
	}
	l.cursor = nil
	return nil
}

// Remove removes all underlying file-based resources for the queue.
// It is an error to call this on an open queue.
func (l *queue) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != nil || l.tail != nil || l.segments != nil {
		return ErrQueueOpen
	}

	return os.RemoveAll(l.dir)
}

// Purge garbage collects the segments that are behind cursor.
func (l *queue) Purge() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) <= 1 {
		// head, curror, tail are in the same segment
		return nil
	}

	for {
		if l.cursor.pos.SegmentID > l.head.id {
			l.trimHead()
		} else {
			return nil
		}

	}
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

// Append appends a block to the end of the queue
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

func (q *queue) Next(b *block) (err error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	c := q.cursor
	if c == nil {
		return ErrNotOpen
	}
	err = c.seg.ReadOne(b)
	switch err {
	case nil:
		c.advanceOffset(b.size())
		return

	case io.EOF:
		// cursor might have:
		// 1. reached end of the current segment: will advance to next segment
		// 2. reached end of tail
		if ok := c.advanceSegment(); !ok {
			return ErrEOQ
		}

		// advanced to next segment, read one block
		err = c.seg.ReadOne(b)
		switch err {
		case nil:
			// bingo!
			c.advanceOffset(b.size())
			return

		case io.EOF:
			// tail is empty
			return ErrEOQ

		default:
			return
		}

	default:
		return
	}
}

func (l *queue) EmptyInflight() bool {
	return l.emptyInflight
}

// diskUsage returns the total size on disk used by the queue
func (l *queue) diskUsage() int64 {
	var size int64
	for _, s := range l.segments {
		size += s.DiskUsage()
	}
	return size
}

// loadSegments loads all segments on disk
func (l *queue) loadSegments() (segments, error) {
	segments := []*segment{}

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return segments, err
	}

	for _, segment := range files {
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

// addSegment creates a new empty segment file
// caller is responsible for the lock
func (l *queue) addSegment() (*segment, error) {
	nextID, err := l.nextSegmentID()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(l.dir, fmt.Sprintf("%020d", nextID))
	segment, err := newSegment(nextID, path, l.maxSegmentSize)
	if err != nil {
		return nil, err
	}

	l.segments = append(l.segments, segment)
	return segment, nil
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
			log.Warn("unexpected segment file: %s", segment.Name())
			continue
		}

		if segmentID > maxID {
			maxID = segmentID
		}
	}

	return maxID + 1, nil
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
