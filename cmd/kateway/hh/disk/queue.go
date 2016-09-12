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

	"github.com/funkygao/golib/sync2"
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

	baseDir      string // the slot this queue is using
	dir          string // Directory to create segments
	clusterTopic clusterTopic

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return an error
	// -1 means unlimited
	maxSize int64

	inflights         sync2.AtomicInt64
	appendN, deliverN sync2.AtomicInt64

	purgeInterval time.Duration
	maxAge        time.Duration

	cursor     *cursor
	head, tail *segment
	segments   segments

	quit          chan struct{}
	emptyInflight sync2.AtomicInt32
}

// newQueue create a queue that will store segments in dir and that will
// consume more than maxSize on disk.
func newQueue(baseDir string, ct clusterTopic, maxSize int64, purgeInterval, maxAge time.Duration) *queue {
	q := &queue{
		clusterTopic:   ct,
		baseDir:        baseDir,
		dir:            ct.TopicDir(baseDir),
		maxSegmentSize: defaultSegmentSize,
		maxSize:        maxSize,
		purgeInterval:  purgeInterval,
		maxAge:         maxAge,
		segments:       segments{},
	}
	q.cursor = newCursor(q)
	return q
}

// Open opens the queue for reading and writing
func (q *queue) Open() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := mkdirIfNotExist(q.dir); err != nil {
		return err
	}

	var (
		minId            uint64 = 0
		moveCursorToHead bool   = false
	)
	if err := q.cursor.open(); err != nil {
		// cursor file might not exist or json file corrupts
		log.Warn("queue[%s] cursor: %s", q.ident(), err)
		moveCursorToHead = true
	} else {
		// load segments from cursor checkpoint
		minId = q.cursor.pos.SegmentID
	}

	segments, err := q.loadSegments(minId)
	if err != nil {
		return err
	}
	q.segments = segments

	if len(q.segments) == 0 {
		// create the 1st segment
		if _, err = q.addSegment(); err != nil {
			return err
		}
	}

	q.head = q.segments[0]
	q.tail = q.segments[len(q.segments)-1]

	// cursor open must be placed below queue open
	if err = q.cursor.initPosition(moveCursorToHead); err != nil {
		return err
	}

	return nil
}

func (q *queue) Start() {
	q.quit = make(chan struct{})

	q.wg.Add(1)
	go q.housekeeping()

	q.wg.Add(1)
	go q.pump()
}

// Close stops the queue for reading and writing
func (q *queue) Close() error {
	close(q.quit)

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, s := range q.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}

	q.head = nil
	q.tail = nil
	q.segments = nil

	q.wg.Wait()
	if err := q.cursor.dump(); err != nil {
		return err
	}
	q.cursor = nil
	return nil
}

func (q *queue) Inflights() int64 {
	return q.inflights.Get()
}

func (q *queue) AppendN() int64 {
	return q.appendN.Get()
}

func (q *queue) DeliverN() int64 {
	return q.deliverN.Get()
}

// Remove removes all underlying file-based resources for the queue.
// It is an error to call this on an open queue.
func (q *queue) Remove() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head != nil || q.tail != nil || q.segments != nil {
		return ErrQueueOpen
	}

	return os.RemoveAll(q.dir)
}

// Purge garbage collects the segments that are behind cursor.
func (q *queue) Purge() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.segments) <= 1 {
		// head, curror, tail are in the same segment
		return nil
	}

	for {
		if q.cursor.pos.SegmentID > q.head.id &&
			q.head.LastModified().Add(q.maxAge).Unix() < time.Now().Unix() {
			q.trimHead()
		} else {
			return nil
		}

	}
}

// LastModified returns the last time the queue was modified.
func (q *queue) LastModified() time.Time {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.tail.LastModified()
}

// Append appends a block to the end of the queue
func (q *queue) Append(b *block) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tail == nil {
		return ErrQueueNotOpen
	}

	if q.maxSize > 0 && q.diskUsage()+b.size() > q.maxSize {
		return ErrQueueFull
	}

	// Append the entry to the tail, if the segment is full,
	// try to create new segment and retry the append
	if err := q.tail.Append(b); err == ErrSegmentFull {
		segment, err := q.addSegment()
		if err != nil {
			return err
		}

		q.tail = segment
		err = q.tail.Append(b)
		if err == nil {
			q.inflights.Add(1)
			q.appendN.Add(1)
		}
		return err
	} else if err != nil {
		return err
	}

	q.appendN.Add(1)
	q.inflights.Add(1)
	return nil
}

func (q *queue) Rollback(b *block) (err error) {
	c := q.cursor
	if err = c.advanceOffset(-b.size()); err != nil {
		return
	}

	// rollback needn't consider cross segment case
	return c.seg.Seek(c.pos.Offset)
}

func (q *queue) Next(b *block) (err error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	c := q.cursor
	if c == nil {
		return ErrQueueNotOpen
	}
	err = c.seg.ReadOne(b)
	switch err {
	case nil:
		return c.advanceOffset(b.size())

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
			return c.advanceOffset(b.size())

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

func (q *queue) EmptyInflight() bool {
	return q.emptyInflight.Get() == 1
}

// diskUsage returns the total size on disk used by the queue
func (q *queue) diskUsage() int64 {
	var size int64
	for _, s := range q.segments {
		size += s.DiskUsage()
	}
	return size
}

// loadSegments loads all in-range segments on disk
// FIXME manage q.inflights counter while loading segments
func (q *queue) loadSegments(minId uint64) (segments, error) {
	segments := []*segment{}

	files, err := ioutil.ReadDir(q.dir)
	if err != nil {
		return segments, err
	}

	for _, segment := range files {
		if segment.IsDir() || segment.Name() == cursorFile {
			continue
		}

		// segment file names are all numeric
		id, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			log.Error("queue[%s] segment:%s %s", q.ident(), segment.Name(), err)
			continue
		}
		if id < minId {
			log.Debug("queue[%s] skip stale segment:%s", q.ident(), segment.Name())
			continue
		}

		segment, err := newSegment(id, filepath.Join(q.dir, segment.Name()), q.maxSegmentSize)
		if err != nil {
			return segments, err
		}

		segments = append(segments, segment)
	}
	return segments, nil
}

// addSegment creates a new empty segment file
// caller is responsible for the lock
func (q *queue) addSegment() (*segment, error) {
	nextID, err := q.nextSegmentID()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(q.dir, fmt.Sprintf("%020d", nextID))
	segment, err := newSegment(nextID, path, q.maxSegmentSize)
	if err != nil {
		return nil, err
	}

	q.segments = append(q.segments, segment)
	return segment, nil
}

// nextSegmentID returns the next segment ID that is free
func (q *queue) nextSegmentID() (uint64, error) {
	segments, err := ioutil.ReadDir(q.dir)
	if err != nil {
		return 0, err
	}

	var maxID uint64
	for _, segment := range segments {
		if segment.IsDir() || segment.Name() == cursorFile {
			continue
		}

		// Segments file names are all numeric
		segmentID, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			log.Warn("unexpected segment file: %s", filepath.Join(q.dir, segment.Name()))
			continue
		}

		if segmentID > maxID {
			maxID = segmentID
		}
	}

	return maxID + 1, nil
}

func (q *queue) ident() string {
	return q.dir
}

func (q *queue) trimHead() (err error) {
	if len(q.segments) <= 1 {
		return ErrHeadIsTail
	}

	q.segments = q.segments[1:]

	if err = q.head.Remove(); err != nil {
		return
	}

	q.head = q.segments[0]
	return
}

// TODO skipCursorSegment skip the current corrupted cursor segment and
// advance to next segment.
// if tail corrupts, add new segment.
func (q *queue) skipCursorSegment() {

}
