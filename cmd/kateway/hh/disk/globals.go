package disk

import (
	"time"

	"github.com/funkygao/golib/timewheel"
	log "github.com/funkygao/log4go"
)

const (
	cursorFile = "cursor.dmp"

	defaultSegmentSize = 100 << 20 // if each block=1k, can hold up to 100k blocks
	maxBlockSize       = 1 << 20

	defaultPurgeInterval = time.Minute * 10
	defaultMaxAge        = time.Hour * 24 * 7
	initialBackoff       = time.Second
	maxBackoff           = time.Second * 31
	defaultMaxQueueSize  = -1 // unlimited
	defaultMaxRetries    = 5
	flusherMaxRetries    = 3
	pollSleep            = time.Second
	dumpPerBlocks        = 100
)

var (
	DisableBufio = true
	Auditor      *log.Logger

	currentMagic = [2]byte{0, 0}

	timer *timewheel.TimeWheel

	// group commit
	flushEveryBlocks = 100
	flushInterval    = time.Second
)
