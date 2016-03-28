package main

import (
	"encoding/json"

	"github.com/funkygao/gafka/mpool"
)

const MaxTagLen = 100

func isTaggedMessage(msg []byte) bool {
	return msg[0] == 0
}

func TagMessage(tags map[string]string, msg []byte) *mpool.Message {
	b, _ := json.Marshal(tags)

	totalLen := len(b) + 1 + len(msg)
	m := mpool.NewMessage(totalLen)
	m.Write([]byte{0}) // mark this message is tagged
	m.Write(b)
	m.Write(msg)

	return m
}

func UntagMessage(msg []byte, tags *map[string]string) ([]byte, error) {
	if !isTaggedMessage(msg) {
		return msg, nil
	}

	var hi int
	for idx, c := range msg[1:] {
		if c == '}' {
			hi = idx + 1
			break
		}
	}

	if hi == 0 {
		return nil, ErrIllegalTaggedMessage
	}

	if err := json.Unmarshal(msg[1:hi+1], tags); err != nil {
		return nil, err
	}
	return msg[hi+1:], nil
}
