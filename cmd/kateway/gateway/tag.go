package gateway

import (
	"bytes"
	"strings"

	"github.com/funkygao/gafka/mpool"
)

const (
	TagMarkStart = byte(1) // FIXME conflicts with ProtocolBuffer
	TagMarkEnd   = byte(2)
	TagSeperator = ";" // follow cookie rules a=b;c=d
)

func IsTaggedMessage(msg []byte) bool {
	return msg[0] == TagMarkStart
}

// TODO perf
func AddTagToMessage(m *mpool.Message, tags string) {
	body := make([]byte, len(m.Body))
	copy(body, m.Body) // FIXME O(N)

	m.Reset()
	m.Write([]byte{TagMarkStart})
	m.WriteString(tags)
	m.Write([]byte{TagMarkEnd})

	m.Write(body)
}

func ExtractMessageTag(msg []byte) ([]string, int, error) {
	tagEnd := bytes.IndexByte(msg, TagMarkEnd)
	if tagEnd == -1 {
		// not a tagged message
		return nil, 0, ErrIllegalTaggedMessage
	}

	tag := string(msg[1:tagEnd]) // discard the tag mark start
	tags := parseMessageTag(tag)
	return tags, tagEnd + 1, nil
}

func tagLen(tag string) int {
	return 2 + len(tag) // TagMarkStart tag TagMarkEnd
}

func parseMessageTag(tag string) []string {
	return strings.Split(strings.TrimSuffix(tag, TagSeperator), TagSeperator)
}
