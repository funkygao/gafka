package main

const (
	HttpHeaderAppid  = "Appid"
	HttpHeaderPubkey = "Pubkey"
	HttpHeaderSubkey = "Subkey"

	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

	UrlQueryKey   = "key"
	UrlQueryReset = "reset"
	UrlQueryAsync = "async"
	UrlQueryDelay = "delay"

	ContentTypeHeader = "Content-Type"
	ContentTypeJson   = "application/json; charset=utf8"
	ContentTypeText   = "text/plain; charset=utf8"
)

var (
	ResponsePubOk = []byte(`{"ok": 1}`)
)
