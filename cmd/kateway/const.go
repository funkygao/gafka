package main

const (
	HttpHeaderAppid         = "Appid"
	HttpHeaderPubkey        = "Pubkey"
	HttpHeaderSubkey        = "Subkey"
	HttpHeaderConnection    = "Connection"
	HttpHeaderXForwardedFor = "X-Forwarded-For"
	HttpHeaderPartition     = "X-Partition"
	HttpHeaderOffset        = "X-Offset"

	UrlParamCluster = "cluster"
	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

	UrlQueryKey   = "key"
	UrlQueryReset = "reset"
	UrlQueryAsync = "async"
	UrlQueryDelay = "delay"
	UrlQueryGroup = "group"

	ContentTypeHeader = "Content-Type"
	ContentTypeText   = "text/plain; charset=utf8"

	CharBraceletLeft  = '{'
	CharBraceletRight = '}'
	CharDot           = '.'

	MaxPartitionKeyLen = 256
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
