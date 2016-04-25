package main

import (
	"os"
)

type LogSegment struct {
	baseOffset int64

	dir os.File

	log   FileMessageSet
	index OffsetIndex
}

func (this *LogSegment) changeFileSuffixes(oldSuffix, newSuffix string) {
	// change both log and index
}

// rebuild the index from the log file and lop off any invalid bytes from the end of the log and index
func (this *LogSegment) recover() {

}

func (this *LogSegment) delete() {
	this.log.delete()
	this.index.delete()
}

func (this *LogSegment) append() {

}
