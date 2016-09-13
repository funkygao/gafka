package gateway

import (
	"strings"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/mpool"
)

func TestAddAndExtractMessageTag(t *testing.T) {
	tag := "a=b;c=d"
	body := "hello world"
	m := mpool.NewMessage(len(body) + tagLen(tag))
	m.Body = m.Body[:len(body)+tagLen(tag)]
	for i := 0; i < len(body); i++ {
		m.Body[i] = body[i]
	}

	t.Logf("%s  %+v %d/%d", string(m.Body), m.Body, len(body), len(m.Body))
	AddTagToMessage(m, tag)
	t.Logf("%s  %+v %d", string(m.Body), m.Body, len(m.Body))
	assert.Equal(t, TagMarkStart, m.Body[0])
	t.Logf("%s", string(m.Body))

	// extract tag
	tags, i, err := ExtractMessageTag(m.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, body, string(m.Body[i:]))
	assert.Equal(t, 2, len(tags))
	t.Logf("%+v", tags)
}

func TestParseMessageTag(t *testing.T) {
	tags := parseMessageTag("a;y_;")
	assert.Equal(t, 2, len(tags))
	assert.Equal(t, "a", tags[0])
	assert.Equal(t, "y_", tags[1])
}

func BenchmarkAddTagToMessage(b *testing.B) {
	b.ReportAllocs()
	m := mpool.NewMessage(1024)
	m.Body = m.Body[:1024]
	tag := "a;c"
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
	tag := "a;c"
	m.Body = []byte(strings.Repeat("X", 900))
	AddTagToMessage(m, tag)
	for i := 0; i < b.N; i++ {
		ExtractMessageTag(m.Body)
	}
	b.SetBytes(int64(len(m.Body)))
}
