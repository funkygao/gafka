package api

import (
	"time"
)

type Config struct {
	AppId  string
	Secret string

	Timeout   time.Duration
	KeepAlive time.Duration

	Debug bool
}

func NewDefaultConfig() *Config {
	return &Config{
		Timeout:   time.Second * 10,
		KeepAlive: time.Minute,
		Debug:     false,
	}
}
