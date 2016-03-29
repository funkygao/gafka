package main

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// /jobs/:topic/:ver?delay=100
func (this *Gateway) addJobHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
}

// /jobs/:id
func (this *Gateway) deleteJobHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
}
