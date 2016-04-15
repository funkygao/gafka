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
		case <-time.After(time.Second * 3):
			if !loaded {
				loadedN = len(events)
				loaded = true
				close(ready)
			}

		case msg := <-s.Messages():
			hook := decode(msg.Value)
			if options.webhookOnly {
				if h, ok := hook.(*Webhook); !ok {
					continue
				} else if options.project != "" && options.project != h.Repository.Name {
					continue
				}
			}

			if options.nonWebhookOnly {
				if _, ok := hook.(*Webhook); ok {
					continue
				}
			}

			lock.Lock()
			events = append(events, hook)
			lock.Unlock()
			if loaded {
				newEvt <- struct{}{}
			}

		case err := <-s.Errors():
			errCh <- err.Err

		case <-quit:
			return
		}
	}

}
