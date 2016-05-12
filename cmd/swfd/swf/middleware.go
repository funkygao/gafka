package swf

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func (this *Swf) Middleware(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		w.Header().Set("Server", "swf")

		// kateway response is always json, including error reponse
		w.Header().Set("Content-Type", "application/json; charset=utf8")

		h(w, r, params)
	}
}
