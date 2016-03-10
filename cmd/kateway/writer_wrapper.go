package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
)

type WriterWrapper interface {
	http.ResponseWriter

	// Status returns the HTTP status of the request, or 0 if one has not
	// yet been sent.
	Status() int

	// BytesWritten returns the total number of bytes sent to the client.
	BytesWritten() int
}

func WrapWriter(w http.ResponseWriter) WriterWrapper {
	_, cn := w.(http.CloseNotifier)
	_, hj := w.(http.Hijacker)
	_, rf := w.(io.ReaderFrom)
	_, fl := w.(http.Flusher)

	bw := basicWriter{ResponseWriter: w}
	if cn && fl && hj && rf {
		return &fancyWriter{flushWriter{bw}}
	}
	if fl {
		return &flushWriter{bw}
	}
	return &bw
}

type basicWriter struct {
	http.ResponseWriter

	wroteHeader bool
	code        int
	bytes       int
}

func (this *basicWriter) Write(buf []byte) (int, error) {
	this.bytes += len(buf)
	return this.ResponseWriter.Write(buf)
}

func (this *basicWriter) WriteHeader(code int) {
	if !this.wroteHeader {
		this.code = code
		this.wroteHeader = true
		this.ResponseWriter.WriteHeader(code)
	}
}

func (this *basicWriter) Status() int {
	if this.code == 0 {
		return http.StatusOK
	}
	return this.code
}

func (this *basicWriter) BytesWritten() int {
	return this.bytes
}

type flushWriter struct {
	basicWriter
}

func (this *flushWriter) Flush() {
	this.ResponseWriter.(http.Flusher).Flush()
}

// fancyWriter is a writer that additionally satisfies http.CloseNotifier,
// http.Flusher, http.Hijacker, and io.ReaderFrom. It exists for the common case
// of wrapping the http.ResponseWriter that package http gives you, in order to
// make the proxied object support the full method set of the proxied object.
type fancyWriter struct {
	flushWriter
}

func (this *fancyWriter) CloseNotify() <-chan bool {
	return this.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (this *fancyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return this.ResponseWriter.(http.Hijacker).Hijack()
}

func (this *fancyWriter) ReadFrom(r io.Reader) (int64, error) {
	return this.ResponseWriter.(io.ReaderFrom).ReadFrom(r)
}
