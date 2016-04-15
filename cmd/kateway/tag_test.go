package main

import (
	"strings"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/mpool"
)

func TestAddAndExtractMessageTag(t *testing.T) {
	m := mpool.NewMessage(1000)
	body := "hello world"
	m.Write([]byte(body))

	t.Logf("%s  %+v %d", string(m.Body), m.Body, len(m.Body))
	AddTagToMessage(m, "a=b;c=d")
	t.Logf("%s  %+v %d", string(m.Body), m.Body, len(m.Body))
	assert.Equal(t, TagMarkStart, m.Body[0])

	// extract tag
	tags, i, err := ExtractMessageTag(m.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, body, string(m.Body[i:]))
	assert.Equal(t, 2, len(tags))
	t.Logf("%+v", tags)
}

func TestParseMessageTag(t *testing.T) {
	tags := parseMessageTag("a=b;xx_=y_;")
	assert.Equal(t, 2, len(tags))
	assert.Equal(t, "a", tags[0].Name)
	assert.Equal(t, "y_", tags[1].Value)
}

func BenchmarkAddTagToMessage(b *testing.B) {
	b.ReportAllocs()
	m := mpool.NewMessage(1024)
	m.Body = m.Body[:1024]
	tag := "a=b;c=d"
	m.Body = []byte(strings.Repeat("X", 900))
	for i := 0; i < b.N; i++ {
		AddTagToMessage(m, tag)
	}
	b.SetBytes(int64(tagLen(tag)) + 900)
}

func BenchmarkExtractMessageTag(b *testing.B) {
	b.ReportAllocs()
	m := mpool.NewMessage(1024)
	m.Body = m.Body[:1024]
	tag := "a=b;c=d"
	m.Body = []byte(strings.Repeat("X", 900))
	AddTagToMessage(m, tag)
	for i := 0; i < b.N; i++ {
		ExtractMessageTag(m.Body)
	}
	b.SetBytes(int64(len(m.Body)))
}
