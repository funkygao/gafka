package main

import (
	"os"
	"strings"
	"sync"
	"time"
)

type Log struct {
	compact       bool
	retentionMs   time.Duration
	retentionSize int64

	mu sync.Mutex

	dir           os.File
	recoveryPoint int64 // offset
	scheduler     *KafkaScheduler

	segments   map[int64]LogSegment // startOffset:LogSegment
	nextOffset int64
}

func (this *Log) loadSegments() {
	files, _ := this.dir.Readdir(0)
	// first pass:  remove any temporary files and complete any interrupted swap operations
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		if strings.HasSuffix(f.Name(), ".deleted") || strings.HasSuffix(f.Name(), ".cleaned") {
			os.Remove(d.Name())
		} else if strings.HasSuffix(f.Name(), ".swap") {
			// we crashed in the middle of a swap operation, to recover:
			// if a log, swap it in and delete the .index file
			// if an index just delete it, it will be rebuilt
		}
	}

	// second pass: load all the .log and .index files
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		switch {
		case strings.HasSuffix(f.Name(), ".index"):
			// if found an orphaned index file, delete it

		case strings.HasSuffix(f.Name(), ".log"):
			start := extractStartOffsetFromLogFilename(f.Name())
			seg := LogSegment{
				dir:                this.dir,
				baseOffset:         start,
				indexIntervalBytes: config.getInt("index.interval.bytes", 4096),
				maxIndexSize:       config.getInt("segment.index.bytes", 2<<10),
			}
			this.segments[start] = seg
		}
	}

	if len(this.segments) == 0 {
		// create an empty segment at offset 0
	} else {
		this.recoverLog()
	}
}

func (this *Log) recoverLog() {

}

func (this *Log) deleteOldSegments(predicate func(LogSegment) bool) int {
	toBeDeleted := make([]LogSegment, 0)
	for _, l := range this.segments {
		if predicate(l) {
			toBeDeleted = append(toBeDeleted, l)
		}
	}

	if len(toBeDeleted) > 0 {
		if len(toBeDeleted) == len(this.segments) {
			// we must always have at least one segment, so if we are going to delete all the segments, create a new one first
			this.roll()
		}

		for _, s := range toBeDeleted {
			this.deleteSegment(s)
		}
	}

	return len(toBeDeleted)
}

func (this *Log) deleteSegment(seg LogSegment) {
	delete(this.segments, seg.baseOffset)
	this.asyncDeleteSegment(seg)
}

func (this *Log) asyncDeleteSegment(seg LogSegment) {
	seg.changeFileSuffixes("", ".deleted")
	this.scheduler.Schedule("delete-file", func() {
		seg.delete()
	}, time.Minute, 0)
}

func (this *Log) roll() {

}

func (this *Log) maybeRoll() *LogSegment {

}

func (this *Log) append(msg MessageSet) {
	segment := this.maybeRoll()
	segment.append()
}
