package main

import (
	"encoding/json"
	"strings"
)

func decode(msg []byte) interface{} {
	var hook interface{}
	event := string(msg)
	switch {
	case strings.HasPrefix(event, `{"event_name":"project_create",`):
		hook = &SystemHookProjectCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_add_to_team"`):
		hook = &SystemHookUserAddToTeam{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_add_to_group"`):
		hook = &SystemHookUserAddToGroup{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"user_create"`):
		hook = &SystemHookUserCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"object_kind":"push"`):
		hook = &Webhook{}
		json.Unmarshal(msg, &hook)

	default:
		hook = &SystemHookUnknown{}
	}

	return hook
}
