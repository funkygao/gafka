package open

import (
	"time"
)

type config struct {
	Zone    string
	Refresh time.Duration
}

func DefaultConfig(zone string) *config {
	return &config{
		Zone:    zone,
		Refresh: time.Minute * 5,
	}
}
