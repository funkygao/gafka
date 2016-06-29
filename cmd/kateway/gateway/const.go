package gateway

const (
	HttpHeaderAppid         = "Appid"
	HttpHeaderPubkey        = "Pubkey"
	HttpHeaderSubkey        = "Subkey"
	HttpHeaderXForwardedFor = "X-Forwarded-For"
	HttpHeaderPartition     = "X-Partition"
	HttpHeaderOffset        = "X-Offset"
	HttpHeaderMsgBury       = "X-Bury"
	HttpHeaderMsgKey        = "X-Key"
	HttpHeaderMsgTag        = "X-Tag"
	HttpHeaderJobId         = "X-Job-Id"

	UrlParamCluster = "cluster"
	UrlParamTopic   = "topic"
	UrlParamVersion = "ver"
	UrlParamAppid   = "appid"
	UrlParamGroup   = "group"

	MaxPartitionKeyLen = 256
)

var (
	ResponseOk = []byte(`{"ok":1}`)
)
