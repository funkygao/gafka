package main

var (
	options struct {
		topic       string
		logfile     string
		webhookOnly bool
		syshookOnly bool
		debug       bool
		mock        bool
		project     string
		noUI        bool
	}
)
