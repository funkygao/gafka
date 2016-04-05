package manager

type WebHook struct {
	Cluster  string
	Topic    string
	Ver      string
	Endpoint string // the webhook's configured URL
}
