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
		PurgeInterval: time.Second * 2,  // FIXME for debug only, should be 1m
		MaxAge:        time.Second * 10, // FIXME for debug only, should be 7d
	}
}

func (this *Config) Validate() error {
	if this.Dir == "" {
		return errors.New("Dir must be specified")
	}

	return nil
}
