package disk

import (
	"bufio"
	"os"
)

type bufferReader struct {
	f      *os.File
	reader *bufio.Reader
}

func newBufferReader(f *os.File) *bufferReader {
	return &bufferReader{
		f:      f,
		reader: bufio.NewReader(f),
	}
}

func (r *bufferReader) Read(b []byte) (n int, err error) {
	if DisableBufio {
		return r.f.Read(b)
	}
	return r.reader.Read(b)
}

func (r *bufferReader) Close() error {
	return r.f.Close()
}

func (r *bufferReader) Seek(offset int64, whence int) (ret int64, err error) {
	if ret, err = r.f.Seek(offset, whence); err != nil {
		return
	}

	if DisableBufio {
		return
	}

	// bufio sync with file
	r.reader.Reset(r.f)
	return
}

func (r *bufferReader) Name() string {
	return r.f.Name()
}

type bufferWriter struct {
	f      *os.File
	writer *bufio.Writer
}

func newBufferWriter(f *os.File) *bufferWriter {
	return &bufferWriter{
		f:      f,
		writer: bufio.NewWriter(f),
	}
}

func (w *bufferWriter) Write(p []byte) (nn int, err error) {
	if DisableBufio {
		return w.f.Write(p)
	}
	return w.writer.Write(p)
}

func (w *bufferWriter) Sync() error {
	if DisableBufio {
		return w.f.Sync()
	}

	if err := w.writer.Flush(); err != nil { // this will greatly impact perf TODO
		return err
	}
	return w.f.Sync()
}

func (w *bufferWriter) Close() error {
	if !DisableBufio {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	w.f.Sync()
	return w.f.Close()
}

func (w *bufferWriter) Name() string {
	return w.f.Name()
}
