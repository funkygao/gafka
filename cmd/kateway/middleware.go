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

		t1 := time.Now()
		h(w, r, params) // Delegate request to the given handle
		if this.accessLogger != nil {
			// r.URL.EscapedPath()
			this.accessLogger.Printf("%s %s", r.RequestURI, time.Since(t1))
		}

	}
}
