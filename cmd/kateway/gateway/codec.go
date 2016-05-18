package gateway

import (
	"encoding/binary"
	"io"
)

func writeI16(writer io.Writer, buf []byte, v int16) error {
	b := buf[:2]
	binary.BigEndian.PutUint16(b, uint16(v))
	_, e := writer.Write(b)
	return e
}

func writeI32(writer io.Writer, buf []byte, v int32) error {
	b := buf[:4]
	binary.BigEndian.PutUint32(b, uint32(v))
	_, e := writer.Write(b)
	return e
}

func writeI64(writer io.Writer, buf []byte, v int64) error {
	b := buf[:8]
	binary.BigEndian.PutUint64(b, uint64(v))
	_, e := writer.Write(b)
	return e
}

type Message struct {
	Partition int32
	Offset    int64
	Value     []byte
}

func DecodeMessageSet(messageSet []byte) []Message {
	r := make([]Message, 0)

	idx := 0
	for {
		m := Message{}
		m.Partition = int32(binary.BigEndian.Uint32(messageSet[idx : idx+4]))

		idx += 4
		m.Offset = int64(binary.BigEndian.Uint64(messageSet[idx : idx+8]))

		idx += 8
		msgLen := int(binary.BigEndian.Uint32(messageSet[idx : idx+4]))

		idx += 4
		m.Value = messageSet[idx : idx+msgLen]

		r = append(r, m)

		idx += msgLen
		if idx == len(messageSet) {
			return r
		}
	}

}
