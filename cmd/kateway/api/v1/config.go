package api

import (
	"time"
)

type Config struct {
	AppId  string
	Secret string

	Timeout   time.Duration
	KeepAlive time.Duration

	Sub struct {
		Scheme   string // https or http
		Endpoint string // host:port
	}

	Pub struct {
		Scheme   string // http or https
		Endpoint string // host:port
	}

	Admin struct {
		Scheme   string
		Endpoint string
	}

	Debug bool
}

func DefaultConfig(appid, secret string) *Config {
	cf := &Config{
		AppId:     appid,
		Secret:    secret,
		Timeout:   time.Second * 120, // FIXME
		KeepAlive: time.Minute,
		Debug:     false,
	}
	cf.Sub.Scheme = "http"
	cf.Pub.Scheme = "http"
	cf.Admin.Scheme = "http"
	return cf
}
