// +build !fasthttp

package gateway

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

func (this *Gateway) middleware(h httprouter.Handle) httprouter.Handle {
	var (
		// GC will touch every single item of the map during mark and scan phase
		// Go 1.5 https://github.com/golang/go/issues/9477
		// TODO map[int64]int
		connections   = make(map[string]int, 500) // remoteAddr:counter
		connectionsMu sync.Mutex
	)

	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		w.Header().Set("Server", "kateway")

		// kateway response is mostly json, including error reponse
		// for non-json response, handler can override this
		w.Header().Set("Content-Type", "application/json; charset=utf8")

		// CORS: cross origin resource sharing
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		// max request per conn to rebalance the session sticky http conns
		if Options.MaxRequestPerConn > 1 {
			maxReqReached := false
			connectionsMu.Lock()
			if n, present := connections[r.RemoteAddr]; present && n >= Options.MaxRequestPerConn {
				maxReqReached = true
				delete(connections, r.RemoteAddr)
			} else {
				connections[r.RemoteAddr]++ // in golang, works even when present=false
			}
			connectionsMu.Unlock()

			if maxReqReached {
				log.Trace("%s max req per conn reached: %d", r.RemoteAddr, Options.MaxRequestPerConn)

				w.Header().Set("Connection", "close")
			}
		}

		if !Options.EnableAccessLog {
			h(w, r, params)

			return
		}

		// TODO latency histogram here
		// TODO slow response recording here

		ww := SniffWriter(w) // sniff the status and content size for logging
		h(ww, r, params)     // delegate request to the given handle

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
		appid = getHttpRemoteIp(r) // cheat appid as remote ip, if not present, use ip
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
