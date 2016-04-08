package main

import (
	"encoding/json"
	"strings"

	"github.com/Shopify/sarama"
)

func subLoop(quit chan struct{}) {
	c, _ := sarama.NewConsumer([]string{
		"10.209.18.65:11004",
		"10.209.18.65:11004",
	}, sarama.NewConfig())
	s, _ := c.ConsumePartition("30.gitlab_events.v1", 0, sarama.OffsetOldest)
	defer s.Close()

	for {
		select {
		case msg := <-s.Messages():
			event := string(msg.Value)
			switch {
			case strings.HasPrefix(event, `{"event_name":"project_create",`):
				hook := &SystemHookProjectCreate{}
				json.Unmarshal(msg.Value, hook)
				events = append(events, hook)

			case strings.HasPrefix(event, `{"event_name":"user_add_to_team"`):
				hook := &SystemHookUserAddToTeam{}
				json.Unmarshal(msg.Value, hook)
				events = append(events, hook)

			case strings.HasPrefix(event, `{"event_name":"user_add_to_group"`):
				hook := &SystemHookUserAddToGroup{}
				json.Unmarshal(msg.Value, hook)
				events = append(events, hook)

			case strings.HasPrefix(event, `{"event_name":"user_create"`):
				hook := &SystemHookUserCreate{}
				json.Unmarshal(msg.Value, hook)
				events = append(events, hook)

			case strings.HasPrefix(event, `{"object_kind":"push"`):
				hook := &Webhook{}
				json.Unmarshal(msg.Value, &hook)
				events = append(events, hook)

			default:
				hook := &SystemHookUnknown{}
				events = append(events, hook)
			}

		case <-quit:
			return
		}
	}

}
