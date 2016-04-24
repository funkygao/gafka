package main

import (
	"os"
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

	segments map[int64]LogSegment // startOffset:LogSegment
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
