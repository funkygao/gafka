package swf

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type apiServer struct {
	*webServer
}

func (this *Swf) checkAliveHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	w.Write(ResponseOk)
}

func (this *apiServer) NotImplemented(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
}
