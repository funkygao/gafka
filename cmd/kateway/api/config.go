package api

import (
	"time"
)

type Config struct {
	Timeout   time.Duration
	KeepAlive time.Duration
	Secret    string
}

func NewDefaultConfig() *Config {
	return &Config{
		Timeout:   time.Second * 10,
		KeepAlive: time.Minute,
	}
}
