package mysql

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/funkygao/gafka/ctx"
)

func kafkaTopicWithStrConcat(m *mysqlStore, appid string, topic string, ver string) string {
	return appid + "." + topic + "." + ver
}

func kafkaTopicWithSprintf(m *mysqlStore, appid string, topic string, ver string) string {
	return fmt.Sprintf("%s.%s.%s", appid, topic, ver)
}

// 456 ns/op	      64 B/op	       4 allocs/op
func BenchmarkKafkaTopicWithSprintf(b *testing.B) {
	m := &mysqlStore{}
	for i := 0; i < b.N; i++ {
		kafkaTopicWithSprintf(m, "appid", "topic", "ver")
	}
}

// 145 ns/op	      16 B/op	       1 allocs/op
func BenchmarkKafkaTopicWithMpool(b *testing.B) {
	m := &mysqlStore{}
	for i := 0; i < b.N; i++ {
		m.KafkaTopic("appid", "topic", "v1")
	}
}

// 145 ns/op	      16 B/op	       1 allocs/op
func BenchmarkKafkaTopicObfuscationWithMpool(b *testing.B) {
	ctx.LoadFromHome()
	m := New(DefaultConfig("local"))
	for i := 0; i < b.N; i++ {
		m.KafkaTopic("appid", "topic", "v10")
	}
}

// 322 ns/op
func BenchmarkMd5Sum(b *testing.B) {
	m := md5.New()
	app := "app1"
	topic := "asdfasdfasfa"
	for i := 0; i < b.N; i++ {
		m.Sum([]byte(app + topic))
	}
}

// 74.4 ns/op	       0 B/op	       0 allocs/op
func BenchmarkKafkaTopicWithStrConcat(b *testing.B) {
	m := &mysqlStore{}
	for i := 0; i < b.N; i++ {
		_ = kafkaTopicWithStrConcat(m, "appid", "topic", "ver")
	}
}
