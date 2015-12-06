package meta

import (
	"testing"
)

func BenchmarkKafkaTopic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		KafkaTopic("appid", "topic", "ver")
	}
}
