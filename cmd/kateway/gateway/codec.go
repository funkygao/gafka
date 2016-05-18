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
