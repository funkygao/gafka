package main

import (
	"net/http"
	"time"

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
		h(w, r, params)

		if this.accessLogger != nil {
			// NCSA Common Log Format (CLF)
			// host ident authuser date request status bytes

			// FIXME status and bytes incorrect
			// TODO replace printf with []byte with sync.Pool, github.com/gorilla/handlers
			this.accessLogger.Printf(`%s - - [%s] "%s %s %s" 200 100`,
				r.Header.Get(HttpHeaderAppid),
				time.Now().Format("02/Jan/2006:15:04:05 -0700"),
				r.Method,
				r.RequestURI, // r.URL.EscapedPath()
				r.Proto)
		}
	}
}
