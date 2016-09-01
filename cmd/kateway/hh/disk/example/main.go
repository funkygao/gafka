package main

import (
	"fmt"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/hh/disk"
	log "github.com/funkygao/log4go"
)

func main() {
	cfg := disk.DefaultConfig()
	cfg.Dir = "hh"
	s := disk.New(cfg)
	if err := s.Start(); err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		if err := s.Append("c1", "t1", "key",
			[]byte(fmt.Sprintf("<#%d sent at: %s>", i+1, time.Now()))); err != nil {
			panic(err)
		}
	}

	log.Info("ENTER sleep...")
	time.Sleep(time.Second)
	s.Stop()
	log.Info("bye!")
	log.Close()
}
