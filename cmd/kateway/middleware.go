// +build !fasthttp

package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/funkygao/gafka/mpool"
	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) MiddlewareKateway(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		w.Header().Set("Server", "kateway")

		// kateway response is always json, including error reponse
		w.Header().Set("Content-Type", "application/json; charset=utf8")

		if !options.EnableAccessLog {
			h(w, r, params)
			return
		}

		// TODO latency histogram here

		// Delegate request to the given handle
		ww := WrapWriter(w) // sniff the status and content size for logging
		h(ww, r, params)

		if this.accessLogger != nil {
			// NCSA Common Log Format (CLF)
			// host ident authuser date request status bytes

			// TODO whitelist
			buf := mpool.AccessLogLineBufferGet()[0:]
			this.accessLogger.Write(this.buildCommonLogLine(buf, r, ww.Status(), ww.BytesWritten()))
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
