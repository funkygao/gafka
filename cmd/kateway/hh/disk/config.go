package disk

import (
	"errors"
	"time"
)

type Config struct {
	Dirs          []string
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
	if len(this.Dirs) == 0 {
		return errors.New("hh Dirs must be specified")
	}

	return nil
}
