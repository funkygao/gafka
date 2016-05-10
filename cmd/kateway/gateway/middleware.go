// +build !fasthttp

package gateway

import (
	"compress/gzip"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) MiddlewareKateway(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		w.Header().Set("Server", "kateway")

		// kateway response is always json, including error reponse
		w.Header().Set("Content-Type", "application/json; charset=utf8")

		var gz *gzip.Writer = nil
		var writer http.ResponseWriter = w
		if Options.EnableGzip && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")

			gz = gzip.NewWriter(w) // TODO only gzip more than N bytes response body
			writer = gzipResponseWriter{Writer: gz, ResponseWriter: w}
		}

		// max request per connetion
		if Options.MaxRequestPerConn > 1 {
			this.connectionsMu.Lock()

			if n, present := this.connections[r.RemoteAddr]; present && n >= Options.MaxRequestPerConn {
				log.Debug("%s max req per conn reached: %d", r.RemoteAddr, n)

				w.Header().Set("Connection", "close")
				delete(this.connections, r.RemoteAddr)
			} else {
				this.connections[r.RemoteAddr]++ // in golang, works even when present=false
			}

			this.connectionsMu.Unlock()
		}

		if !Options.EnableAccessLog {
			h(writer, r, params)

			if gz != nil {
				gz.Close()
			}

			return
		}

		// TODO latency histogram here

		ww := SniffWriter(writer) // sniff the status and content size for logging
		h(ww, r, params)          // delegate request to the given handle

		if gz != nil {
			gz.Close()
		}

		if this.accessLogger != nil {
			// NCSA Common Log Format (CLF)
			// host ident authuser date request status bytes

			// TODO whitelist
			buf := mpool.AccessLogLineBufferGet()[0:]
			this.accessLogger.Log(this.buildCommonLogLine(buf, r, ww.Status(), ww.BytesWritten()))
			mpool.AccessLogLineBufferPut(buf)
		}
	}
}

func (this *Gateway) buildCommonLogLine(buf []byte, r *http.Request, status, size int) []byte {
	appid := r.Header.Get(HttpHeaderAppid)
	if appid == "" {
		appid = getHttpRemoteIp(r)
	}

	buf = append(buf, appid...)
	buf = append(buf, " - - ["...)
	buf = append(buf, time.Now().Format("02/Jan/2006:15:04:05 -0700")...)
	buf = append(buf, `] "`...)
	buf = append(buf, r.Method...)
	buf = append(buf, ' ')
	buf = append(buf, r.RequestURI...)
	buf = append(buf, ' ')
	buf = append(buf, r.Proto...)
	buf = append(buf, `" `...)
	buf = append(buf, strconv.Itoa(status)...)
	buf = append(buf, (" " + strconv.Itoa(size))...)
	buf = append(buf, "\n"...)
	return buf
}
