package main

import (
	"net/http"
)

func (this *Gateway) subHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
}
