package main

import (
	"fmt"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/hh/disk"
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
			[]byte(fmt.Sprintf("value %d sent at: %s", i+1, time.Now()))); err != nil {
			panic(err)
		}
	}

	println("enter sleep...")
	time.Sleep(time.Second * 3)
	s.Stop()
}
