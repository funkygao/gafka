package zk

import (
	"time"
)

type Config struct {
	Name         string
	ZkAddrs      string
	Timeout      time.Duration
	PanicOnError bool
}

func DefaultConfig(name, addrs string) *Config {
	return &Config{
		Name:         name,
		ZkAddrs:      addrs,
		Timeout:      time.Minute,
		PanicOnError: false,
	}
}
