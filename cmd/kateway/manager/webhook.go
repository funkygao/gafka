package manager

type WebHook struct {
	Cluster   string
	Topic     string
	Ver       string
	Endpoints []string // the webhook's configured URLs
}
