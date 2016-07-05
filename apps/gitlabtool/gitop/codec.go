package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

func eventContent(evt interface{}) string {
	var row string
	switch hook := evt.(type) {
	case *Webhook:
		if len(hook.Commits) == 0 {
			row = fmt.Sprintf("%s %s",
				hook.User_name,
				hook.Repository.Name)
		} else {
			commit := hook.Commits[len(hook.Commits)-1] // the most recent commit
			row = fmt.Sprintf("%s %s %s",
				hook.User_name,
				hook.Repository.Name,
				commit.Message)
		}

	case *SystemHookProjectCreate:
		row = fmt.Sprintf("%s created project(%s)",
			hook.Owner_name,
			hook.Name)

	case *SystemHookProjectDestroy:
		row = fmt.Sprintf("%s destroy project(%s)",
			hook.Owner_name,
			hook.Path_with_namespace)

	case *SystemHookGroupCreate:
		row = fmt.Sprintf("%s created group(%s)",
			hook.Owner_name,
			hook.Name)

	case *SystemHookGroupDestroy:
		row = fmt.Sprintf("%s destroy group(%s)",
			hook.Owner_name,
			hook.Name)

	case *SystemHookUserCreate:
		row = fmt.Sprintf("%s %s signup",
			hook.Name,
			hook.Email)

	case *SystemHookUserAddToGroup:
		row = fmt.Sprintf("%s join group(%s)",
			hook.User_name,
			hook.Group_name)

	case *SystemHookUserRemovedFromGroup:
		row = fmt.Sprintf("%s kicked from group(%s)",
			hook.User_name,
			hook.Group_name)

	case *SystemHookUserAddToTeam:
		row = fmt.Sprintf("%s join project(%s)",
			hook.User_name,
			hook.Project_name)

	case *SystemHookUserRemovedFromTeam:
		row = fmt.Sprintf("%s kicked from project(%s)",
			hook.User_name,
			hook.Project_name)

	case *SystemHookKeyCreate:
		row = fmt.Sprintf("%s create ssh key",
			hook.Username)

	case *SystemHookKeyDesctroy:
		row = fmt.Sprintf("%s destroy ssh key",
			hook.Username)

	case *SystemHookUnknown:
		row = fmt.Sprintf("unknown %s", hook.Evt)
	}

	return row
}

func decode(msg []byte) interface{} {
	var hook interface{}
	event := string(msg)
	if options.debug {
		log.Println(event)
	}
	switch {
	case strings.HasPrefix(event, `{"event_name":"project_create"`):
		hook = &SystemHookProjectCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"group_create"`):
		hook = &SystemHookGroupCreate{}
		json.Unmarshal(msg, hook)

	case strings.HasPrefix(event, `{"event_name":"group_destroy"`):
		hook = &SystemHookGroupDestroy{}
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
