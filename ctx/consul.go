package ctx

import (
	"time"
)

type Consul struct {
	Addr          string
	KVPath        string
	TagPrefix     string
	ServiceName   string
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}

func NewConsul() *Consul {
	return &Consul{
		CheckInterval: time.Second * 10,
		CheckTimeout:  time.Second * 30,
	}
}
