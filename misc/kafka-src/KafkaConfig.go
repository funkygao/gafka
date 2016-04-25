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

func (this *KafkaConfig) getBool(key string, defaults bool) bool {
	return defaults
}

func (this *KafkaConfig) getInt(key string, defaults int) int {
	return defaults
}

var config KafkaConfig
