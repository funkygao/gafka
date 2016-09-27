// +build !fasthttp

package gateway

import (
	"net/http"

	//"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/httprouter"
)

//go:generate goannotation $GOFILE
// @rest GET /v1/auth
func (this *pubServer) authHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		appid  = r.Header.Get("X-App-Id")
		secret = r.Header.Get("X-App-Secret")
	)

	// TODO manager auth first

	tokenString, err := jwtToken(appid, secret)
	if err != nil {
		// TODO
		return
	}

	w.Write([]byte(tokenString))
}
