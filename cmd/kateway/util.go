package main

import (
	"net/http"
)

func diff(l1, l2 []string) (added []string, deleted []string) {
	return
}

func writeAuthFailure(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("invalid pubkey"))
}

func writeBreakerOpen(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadGateway)
	w.Write([]byte("circuit broken"))
}

func writeBadRequest(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadRequest)
}

func isBreakeableError(err error) bool {
	if err != ErrTooManyConsumers && err != ErrRebalancing {
		return true
	}

	return false
}
