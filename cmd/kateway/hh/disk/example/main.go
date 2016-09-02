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

	for seq := 0; seq < 5; seq++ {
		go func(seq int) {
			for i := 0; i < 5; i++ {
				if err := s.Append("c1", "t1", []byte("key"),
					[]byte(fmt.Sprintf("<#%d/%d sent at: %s>", seq, i+1, time.Now()))); err != nil {
					panic(err)
				}
			}
		}(seq)
	}

	log.Info("ENTER sleep...")
	time.Sleep(time.Second)
	s.Stop()
	log.Info("bye!")
	log.Close()
}
