package disk

import (
	"errors"
	"time"
)

type Config struct {
	Dir           string // TODO []string to load balance disk IO
	PurgeInterval time.Duration
	MaxAge        time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		PurgeInterval: defaultPurgeInterval,
		MaxAge:        defaultMaxAge,
	}
}

func (this *Config) Validate() error {
	if this.Dir == "" {
		return errors.New("Dir must be specified")
	}

	return nil
}
