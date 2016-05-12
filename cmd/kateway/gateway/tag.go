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

	TagOperatorEqual       TagOperator = "="
	TagOperatorNotEqual    TagOperator = "!"
	TagOperatorGreaterThan TagOperator = ">"
	TagOperatorLessThan    TagOperator = "<"
)

type TagOperator string

type MsgTag struct {
	Name     string
	Value    string
	Operator TagOperator
}

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

func ExtractMessageTag(msg []byte) ([]MsgTag, int, error) {
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

func parseMsgTagValue(raw string, allowDoubleQuote bool) (string, bool) {
	// Strip the quotes, if present.
	if allowDoubleQuote && len(raw) > 1 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		raw = raw[1 : len(raw)-1]
	}

	return raw, true
}

func parseMessageTag(tag string) []MsgTag {
	tags := []MsgTag{}
	kvs := strings.Split(strings.TrimSpace(tag), TagSeperator)
	if len(kvs) == 1 && kvs[0] == "" {
		return nil
	}

	for _, kv := range kvs {
		kv = strings.TrimSpace(kv)
		j := strings.Index(kv, "=")
		if j < 0 {
			continue
		}
		name, value := kv[:j], kv[j+1:]
		value, ok := parseMsgTagValue(value, true)
		if !ok {
			continue
		}

		tags = append(tags, MsgTag{
			Name:  name,
			Value: value,
		})
	}

	return tags
}
