package main

import (
	"time"

	"github.com/Shopify/sarama"
)

func subLoop() {
	c, _ := sarama.NewConsumer([]string{
		"10.209.18.65:11004",
		"10.209.18.65:11004",
	}, sarama.NewConfig())
	s, _ := c.ConsumePartition(options.topic, 0, sarama.OffsetOldest)
	defer s.Close()

	loaded := false
	for {
		select {
		case <-time.After(time.Second * 5):
			if !loaded {
				loaded = true
				close(ready)
			}

		case msg := <-s.Messages():
			hook := decode(msg.Value)

			lock.Lock()
			events = append(events, hook)
			lock.Unlock()
			if loaded {
				newEvt <- struct{}{}
			}

		case <-quit:
			return
		}
	}

}
