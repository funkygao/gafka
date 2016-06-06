package zkmeta

import (
	"time"
)

type config struct {
	Refresh time.Duration
}

func DefaultConfig() *config {
	return &config{
		Refresh: time.Minute * 10,
	}
}
