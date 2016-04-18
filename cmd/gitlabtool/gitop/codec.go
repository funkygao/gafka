package main

import (
	"encoding/json"
	"log"
	"strings"
	"time"
)

func decode(msg []byte) interface{} {
	var hook interface{}
	event := string(msg)
	log.Println(event)
	switch {
	case strings.HasPrefix(event, `{"event_name":"project_create"`):
		hook = &SystemHookProjectCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"group_create"`):
		hook = &SystemHookGroupCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_add_to_team"`):
		hook = &SystemHookUserAddToTeam{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_remove_from_team"`):
		hook = &SystemHookUserRemovedFromTeam{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_add_to_group"`):
		hook = &SystemHookUserAddToGroup{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_remove_from_group"`):
		hook = &SystemHookUserRemovedFromGroup{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_create"`):
		hook = &SystemHookUserCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"project_destroy"`):
		hook = &SystemHookProjectDestroy{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"key_create"`):
		hook = &SystemHookKeyCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"key_destroy"`):
		hook = &SystemHookKeyDesctroy{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"object_kind":"push"`):
		hook = &Webhook{}
		json.Unmarshal(msg, &hook)
		h := hook.(*Webhook)
		if len(h.Commits) == 0 {
			h.ctime = time.Now()
		}

	default:
		hook = &SystemHookUnknown{Evt: event}
	}

	return hook
}
