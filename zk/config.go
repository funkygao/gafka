package zk

import (
	"time"
)

type Config struct {
	ZkAddrs      string
	Timeout      time.Duration
	PanicOnError bool
	LogLevel     string
}

func DefaultConfig(addrs string) *Config {
	return &Config{
		ZkAddrs:      addrs,
		Timeout:      time.Minute,
		PanicOnError: true,
		LogLevel:     "info",
	}
}
