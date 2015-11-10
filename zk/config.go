package zk

import (
	"time"
)

type Config struct {
	Addrs        string
	Timeout      time.Duration
	PanicOnError bool
}

func DefaultConfig(addrs string) *Config {
	return &Config{
		Addrs:        addrs,
		Timeout:      time.Minute,
		PanicOnError: true,
	}
}
