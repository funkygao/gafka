package main

const (
	HttpHeaderAppid         = "Appid"
	HttpHeaderPubkey        = "Pubkey"
	HttpHeaderSubkey        = "Subkey"
	HttpHeaderXForwardedFor = "X-Forwarded-For"
	HttpHeaderPartition     = "X-Partition"
	HttpHeaderOffset        = "X-Offset"
	HttpHeaderMsgBury       = "X-Bury"
	HttpHeaderMsgKey        = "X-Key"
	HttpHeaderJobId         = "X-Job-Id"
	HttpHeaderOrigin        = "X-Origin"

	UrlParamCluster = "cluster"
	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

	SmoketestGroup  = "__smoketest__"
	OriginSmoketest = "smoketest"

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
