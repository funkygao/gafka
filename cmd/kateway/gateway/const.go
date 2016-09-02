package gateway

const (
	HttpHeaderXForwardedFor   = "X-Forwarded-For"
	HttpHeaderPartition       = "X-Partition"
	HttpHeaderOffset          = "X-Offset"
	HttpHeaderMsgBury         = "X-Bury"
	HttpHeaderMsgKey          = "X-Key"
	HttpHeaderMsgTag          = "X-Tag"
	HttpHeaderJobId           = "X-Job-Id"
	HttpHeaderAcceptEncoding  = "Accept-Encoding"
	HttpHeaderContentEncoding = "Content-Encoding"
	HttpEncodingGzip          = "gzip"

	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

	MaxPartitionKeyLen = 256
)

var (
	ResponseOk = []byte(`{"ok":1}`)

	HttpHeaderAppid  = "Appid"
	HttpHeaderPubkey = "Pubkey"
	HttpHeaderSubkey = "Subkey"
)
