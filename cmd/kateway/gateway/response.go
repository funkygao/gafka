package gateway

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func punishClient() {
	time.Sleep(Options.BadClientPunishDuration)
}

func _writeErrorResponse(w http.ResponseWriter, err string, code int) {
	http.Error(w, `{"errmsg":"`+err+`"}`, code)
}

func writeNotFound(w http.ResponseWriter) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

func writeAuthFailure(w http.ResponseWriter, err error) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, err.Error(), http.StatusUnauthorized)
}

func writeWsError(ws *websocket.Conn, err string) {
	ws.WriteMessage(websocket.CloseMessage, []byte(err))
}

func writeQuotaExceeded(w http.ResponseWriter) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, "quota exceeded", http.StatusNotAcceptable)
}

func writeServerError(w http.ResponseWriter, err string) {
	_writeErrorResponse(w, err, http.StatusInternalServerError)
}

func writeBadRequest(w http.ResponseWriter, err string) {
	punishClient()

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, err, http.StatusBadRequest)
}
