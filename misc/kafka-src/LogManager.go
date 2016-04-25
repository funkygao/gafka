package main

import (
	"log"
	"os"
	"time"
)

/*
			   TopicAndPartition	  baseOffset
	LogManager -----------------> Log ----------> LogSegment ---> FileMessageSet

*/
type LogManager struct {
	logDirs                  []os.File
	logs                     map[TopicAndPartition]Log
	recoveryPointCheckpoints map[os.File]map[TopicAndPartition]int64 // dir: {topicPartition: offset}

	zkClient       *ZkClient
	kafkaScheduler *KafkaScheduler
	cleaner        *LogCleaner
}

func (this *LogManager) Startup() {
	this.zkClient.FetchAllTopicConfigs()

	this.kafkaScheduler.Schedule("kafka-log-retention", this.cleanupLogs, time.Second*30,
		config.getDuration("log.retention.check.interval.ms", time.Minute*5))
	this.kafkaScheduler.Schedule("kafka-log-flusher", this.flushDirtyLogs, 0, 0)
	this.kafkaScheduler.Schedule("kafka-recovery-point-checkpoint", this.checkpointRecoveryPointOffsets, 0, 0)

	if this.cleanerEnabled {
		this.cleaner.Startup()
	}
}

func (this *LogManager) cleanupLogs() {
	totalN := 0
	for _, l := range this.logs {
		if l.compact {
			// compact log will NOT be cleaned up
			continue
		}

		log.Printf("Garbage collection %s", l.dir.Name())
		totalN += this.cleanupExpiredSegments(l) + this.cleanupSegmentsToMaintainSize(l)
	}

	log.Printf("Log cleanup completed. %d files deleted", totalN)
}

func (this *LogManager) cleanupExpiredSegments(l Log) int {
	l.deleteOldSegments(func(s LogSegment) bool {
		stat, _ := s.log.file.Stat()
		return time.Since(stat.ModTime()) > l.retentionMs
	})
}

func (this *LogManager) cleanupSegmentsToMaintainSize(l Log) int {

}

func (this *LogManager) flushDirtyLogs() {

}

func (this *LogManager) checkpointRecoveryPointOffsets() {

}

func (this *LogManager) loadLogs() {
	for _, dir := range this.logDirs {
		subdirs, _ := dir.Readdir(dir.Name())
		for _, d := range subdirs {
			if !d.IsDir() {
				continue
			}

			log.Printf("Loading log %s", d.Name())
			// from the log dir name, got the TopicPartition
			tp := TopicAndPartition{}
			this.logs[tp] = Log{
				dir:           d,
				recoveryPoint: this.recoveryPointCheckpoints[dir][tp],
				scheduler:     this.kafkaScheduler,
			}
			this.logs[tp].loadSegments()
		}
	}

}

func (this *LogManager) nextLogDir() *os.File {
	// choose the directory with the least logs in it
	return nil
}

func (this *LogManager) createAndValidateLogDirs() {

}
