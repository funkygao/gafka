package main

import (
	"time"
)

type KafkaScheduler struct {
}

func (this *KafkaScheduler) Startup() {

}

func (this *KafkaScheduler) Schedule(name string, f func(), delay time.Duration, period time.Duration) {

}
