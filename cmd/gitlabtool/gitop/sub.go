package main

import (
	"log"
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
				if options.noUI {
					log.Println("events loaded, ready for new events...")
				}
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

			if options.syshookOnly {
				if _, ok := hook.(*Webhook); ok {
					continue
				}
			}

			if options.noUI {
				if loaded {
					event := eventContent(msg)
					log.Println(event)
					displayNotify(event, "Glass")
				}

				continue
			}

			lock.Lock()
			events = append(events, hook)
			lock.Unlock()
			if loaded {
				newEvt <- hook
			}

		case err := <-s.Errors():
			errCh <- err.Err

		case <-quit:
			return
		}
	}

}
