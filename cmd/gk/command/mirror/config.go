package mirror

type Config struct {
	Z1, Z2         string
	C1, C2         string
	ExcludedTopics map[string]struct{}
	Compress       string
	BandwidthLimit int64
	Debug          bool
	AutoCommit     bool
	ProgressStep   int64
}

func DefaultConfig() *Config {
	return &Config{
		Debug:          false,
		AutoCommit:     true,
		ProgressStep:   5000,
		ExcludedTopics: make(map[string]struct{}),
	}
}
