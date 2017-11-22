package monitor

import (
	"encoding/json"
	"net/http"
	"time"
)

var (
	ResponseOk = []byte(`{"ok":1}`)
)

func writeServerError(w http.ResponseWriter, err string) {
	time.Sleep(time.Second) // todo, configurable

	_writeErrorResponse(w, err, http.StatusInternalServerError)
}

func _writeErrorResponse(w http.ResponseWriter, err string, code int) {
	var out = map[string]string{
		"errmsg": err,
	}
	b, _ := json.Marshal(out)

	http.Error(w, string(b), code)
}

func writeBadRequest(w http.ResponseWriter, err string) {
	time.Sleep(time.Second) // todo, configurable

	w.Header().Set("Connection", "close")
	_writeErrorResponse(w, err, http.StatusBadRequest)
}
