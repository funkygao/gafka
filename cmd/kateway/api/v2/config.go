package api

import (
	"time"
)

type Config struct {
	Timeout    time.Duration
	MaxRetries int
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) WithTimeout(timeout time.Duration) *Config {
	c.Timeout = timeout
	return c
}

func (c *Config) WithMaxRetries(max int) *Config {
	c.MaxRetries = max
	return c
}

func (c *Config) MergeIn(cfgs ...*Config) {
	for _, other := range cfgs {
		mergeInConfig(c, other)
	}
}

func (c *Config) Copy(cfgs ...*Config) *Config {
	dst := NewConfig()
	dst.MergeIn(c)
	dst.MergeIn(cfgs...)
	return dst
}

func mergeInConfig(dst *Config, src *Config) {
}
