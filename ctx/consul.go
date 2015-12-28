package ctx

import (
	"time"
)

type Consul struct {
	Addr          string // address of the Consul server
	KVPath        string
	TagPrefix     string
	ServiceName   string
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}

func NewConsul(consulAddr, serviceName string) *Consul {
	return &Consul{
		Addr:          consulAddr,
		CheckInterval: time.Second * 10,
		CheckTimeout:  time.Second * 30,
		ServiceName:   serviceName,
	}
}
