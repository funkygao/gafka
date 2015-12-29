package main

const (
	HttpHeaderAppid  = "Appid"
	HttpHeaderPubkey = "Pubkey"
	HttpHeaderSubkey = "Subkey"

	UrlParamCluster = "cluster"
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

	CharBraceletLeft  = '{'
	CharBraceletRight = '}'
	CharDot           = '.'
)

var (
	ResponseOk = []byte(`{"ok": 1}`)
)

const (
	logo = `
    _/    _/              _/                                                        
   _/  _/      _/_/_/  _/_/_/_/    _/_/    _/      _/      _/    _/_/_/  _/    _/   
  _/_/      _/    _/    _/      _/_/_/_/  _/      _/      _/  _/    _/  _/    _/    
 _/  _/    _/    _/    _/      _/          _/  _/  _/  _/    _/    _/  _/    _/     
_/    _/    _/_/_/      _/_/    _/_/_/      _/      _/        _/_/_/    _/_/_/      
                                                                           _/       
                                                                      _/_/          
	`
)
