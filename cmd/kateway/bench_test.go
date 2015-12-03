package main

import (
	"testing"
)

func BenchmarkKafkaTopic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		kafkaTopic("appid", "topic", "ver")
	}
}
