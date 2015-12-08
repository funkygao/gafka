package meta

import (
	"fmt"
	"testing"
)

func kafkaTopicWithStrConcat(appid string, topic string, ver string) string {
	return appid + "." + topic + "." + ver
}

func kafkaTopicWithSprintf(appid string, topic string, ver string) string {
	return fmt.Sprintf("%s.%s.%s", appid, topic, ver)
}

// 456 ns/op	      64 B/op	       4 allocs/op
func BenchmarkKafkaTopicWithSprintf(b *testing.B) {

	for i := 0; i < b.N; i++ {
		kafkaTopicWithSprintf("appid", "topic", "ver")
	}
}

// 145 ns/op	      16 B/op	       1 allocs/op
func BenchmarkKafkaTopicWithMpool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		KafkaTopic("appid", "topic", "ver")
	}
}

// 74.4 ns/op	       0 B/op	       0 allocs/op
func BenchmarkKafkaTopicWithStrConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = kafkaTopicWithStrConcat("appid", "topic", "ver")
	}
}
