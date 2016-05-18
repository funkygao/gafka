package gateway

import (
	"bytes"
	"testing"

	"github.com/funkygao/assert"
)

func TestDecodeMessageSetSingleMessage(t *testing.T) {
	w := bytes.NewBuffer(make([]byte, 0))
	partition, offset, msg := int32(1), int64(9087), []byte("hello world")

	buf := make([]byte, 8)
	writeI32(w, buf, partition)
	writeI64(w, buf, offset)
	writeI32(w, buf, int32(len(msg)))
	w.Write(msg)

	t.Logf("%+v", w.Bytes())

	msgSet := DecodeMessageSet(w.Bytes())
	t.Logf("%+v", msgSet)

	assert.Equal(t, 1, len(msgSet))
	assert.Equal(t, partition, msgSet[0].Partition)
	assert.Equal(t, offset, msgSet[0].Offset)
	assert.Equal(t, msg, msgSet[0].Value)
}

func TestDecodeMessageSetMultiMessage(t *testing.T) {
	w := bytes.NewBuffer(make([]byte, 0))
	partition1, offset1, msg1 := int32(1), int64(9087), []byte("hello world")
	partition2, offset2, msg2 := int32(0), int64(1298), []byte("good morning")

	buf := make([]byte, 8)

	// msg1
	writeI32(w, buf, partition1)
	writeI64(w, buf, offset1)
	writeI32(w, buf, int32(len(msg1)))
	w.Write(msg1)

	// msg2
	writeI32(w, buf, partition2)
	writeI64(w, buf, offset2)
	writeI32(w, buf, int32(len(msg2)))
	w.Write(msg2)

	msgSet := DecodeMessageSet(w.Bytes())

	assert.Equal(t, 2, len(msgSet))
	assert.Equal(t, partition1, msgSet[0].Partition)
	assert.Equal(t, offset2, msgSet[1].Offset)
	assert.Equal(t, msg2, msgSet[1].Value)
}
