package disk

import (
	"time"

	log "github.com/funkygao/log4go"
)

const (
	cursorFile = "cursor.dmp"

	defaultSegmentSize = 10 << 20
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

	flushEveryBlocks = 10
	flushInterval    = time.Second
)
