package main

import (
	"encoding/json"
	"strings"
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

	readyForStream := false
	for {
		select {
		case <-time.After(time.Second * 10):
			readyForStream = true

		case msg := <-s.Messages():
			event := string(msg.Value)
			var hook interface{}
			switch {
			case strings.HasPrefix(event, `{"event_name":"project_create",`):
				hook = &SystemHookProjectCreate{}
				json.Unmarshal(msg.Value, hook)

			case strings.HasPrefix(event, `{"event_name":"user_add_to_team"`):
				hook = &SystemHookUserAddToTeam{}
				json.Unmarshal(msg.Value, hook)

			case strings.HasPrefix(event, `{"event_name":"user_add_to_group"`):
				hook = &SystemHookUserAddToGroup{}
				json.Unmarshal(msg.Value, hook)

			case strings.HasPrefix(event, `{"event_name":"user_create"`):
				hook = &SystemHookUserCreate{}
				json.Unmarshal(msg.Value, hook)

			case strings.HasPrefix(event, `{"object_kind":"push"`):
				hook = &Webhook{}
				json.Unmarshal(msg.Value, &hook)

			default:
				hook = &SystemHookUnknown{}

			}

			lock.Lock()
			events = append(events, hook)
			lock.Unlock()
			if readyForStream {
				newEvt <- struct{}{}
			}

		case <-quit:
			return
		}
	}

}
