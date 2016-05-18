package gateway

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/websocket"
)

func writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	http.Error(w, string(b), code)
}

func writeAuthFailure(w http.ResponseWriter, err error) {
	// close the suspicous http connection
	w.Header().Set("Connection", "close")

	writeErrorResponse(w, err.Error(), http.StatusUnauthorized)
}

func writeWsError(ws *websocket.Conn, err string) {
	ws.WriteMessage(websocket.CloseMessage, []byte(err))
}

func writeQuotaExceeded(w http.ResponseWriter) {
	w.Header().Set("Connection", "close")

	writeErrorResponse(w, "quota exceeded", http.StatusNotAcceptable)
}

func writeServerError(w http.ResponseWriter, err string) {
	writeErrorResponse(w, err, http.StatusInternalServerError)
}

func writeBadRequest(w http.ResponseWriter, err string) {
	w.Header().Set("Connection", "close")

	writeErrorResponse(w, err, http.StatusBadRequest)
}
