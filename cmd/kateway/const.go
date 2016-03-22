package main

const (
	HttpHeaderAppid         = "Appid"
	HttpHeaderPubkey        = "Pubkey"
	HttpHeaderSubkey        = "Subkey"
	HttpHeaderXForwardedFor = "X-Forwarded-For"
	HttpHeaderPartition     = "X-Partition"
	HttpHeaderOffset        = "X-Offset"
	HttpHeaderMsgMove       = "X-Move"
	HttpHeaderMsgKey        = "X-Key"

	UrlParamCluster = "cluster"
	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

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
