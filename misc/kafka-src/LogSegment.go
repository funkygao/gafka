package main

type LogSegment struct {
	baseOffset int64

	log   FileMessageSet
	index OffsetIndex
}

func (this *LogSegment) changeFileSuffixes(oldSuffix, newSuffix string) {
	// change both log and index
}

func (this *LogSegment) delete() {
	this.log.delete()
	this.index.delete()
}
