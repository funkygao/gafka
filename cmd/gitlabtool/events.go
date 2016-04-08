package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/gofmt"
	// /"github.com/funkygao/golib/color"
)

//GitlabRepository represents repository information from the webhook
type GitlabRepository struct {
	Name, Url, Description, Home string
}

//Commit represents commit information from the webhook
type Commit struct {
	Id, Message, Timestamp, Url string
	Author                      Author
}

//Author represents author information from the webhook
type Author struct {
	Name, Email string
}

//Webhook represents push information from the webhook
type Webhook struct {
	Ref, User_name      string
	User_id, Project_id int
	Repository          GitlabRepository
	Commits             []Commit
	Total_commits_count int
}

type SystemHookProjectCreate struct {
	Created_at       string
	Name, Owner_name string
}

type SystemHookUserCreate struct {
	Created_at, Name, Email string
}

type SystemHookUserAddToGroup struct {
	Created_at, Group_name, User_name, User_email string
}

type SystemHookUserAddToTeam struct {
	Created_at, Project_name, User_name, User_email string
}

func main() {
	c, _ := sarama.NewConsumer([]string{
		"10.209.18.65:11004",
		"10.209.18.65:11004",
	}, sarama.NewConfig())
	s, _ := c.ConsumePartition("30.gitlab_events.v1", 0, sarama.OffsetOldest)
	defer s.Close()

	var i int
	for msg := range s.Messages() {
		i++
		fmt.Printf("%4d ", i)

		event := string(msg.Value)
		switch {
		case strings.Contains(event, `"event_name":"project_create",`):
			hook := &SystemHookProjectCreate{}
			json.Unmarshal(msg.Value, hook)
			fmt.Printf("%15s %15s %10s %s", hook.Owner_name, since(hook.Created_at), "create", hook.Name)

		case strings.Contains(event, `"event_name":"user_add_to_team"`):
			hook := &SystemHookUserAddToTeam{}
			json.Unmarshal(msg.Value, hook)
			fmt.Printf("%15s %15s %10s %s", hook.User_name, since(hook.Created_at), "join", hook.Project_name)

		case strings.Contains(event, `"event_name":"user_add_to_group"`):
			hook := &SystemHookUserAddToGroup{}
			json.Unmarshal(msg.Value, hook)
			fmt.Printf("%15s %15s %10s %s", hook.User_name, since(hook.Created_at), "group", hook.Group_name)

		case strings.Contains(event, `"event_name":"user_create"`):
			hook := &SystemHookUserCreate{}
			json.Unmarshal(msg.Value, hook)
			fmt.Printf("%15s %15s %10s %s", hook.Name, since(hook.Created_at), "signup", hook.Email)

		case strings.Contains(event, `"object_kind":"push"`):
			hook := &Webhook{}
			json.Unmarshal(msg.Value, &hook)
			fmt.Printf("%15s %10s %s %d\n", hook.User_name, "push", hook.Repository.Name, hook.Total_commits_count)
			for i, c := range hook.Commits {
				fmt.Printf("%18d %15s %s", i, since(c.Timestamp), c.Message)

			}

		default:
			fmt.Printf("%15s", "unknown")

		}

		fmt.Println()
	}

}

func since(timestamp string) string {
	t, _ := time.Parse(time.RFC3339, timestamp)
	return gofmt.PrettySince(t)
}
