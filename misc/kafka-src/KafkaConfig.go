package main

import (
	"time"
)

type KafkaConfig struct {
	brokerId int
	logDirs  []string
}

func (this *KafkaConfig) getDuration(key string, defaults time.Duration) time.Duration {
	return defaults
}

var config KafkaConfig
