package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
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
	Before, After, Ref, User_name string
	User_id, Project_id           int
	Repository                    GitlabRepository
	Commits                       []Commit
	Total_commits_count           int
}

func main() {
	c, err := sarama.NewConsumer([]string{
		"10.209.18.65:11004",
		"10.209.18.65:11004",
	}, sarama.NewConfig())
	s, err := c.ConsumePartition("30.gitlab_events.v1", 0, sarama.OffsetOldest)
	defer s.Close()

	var hook Webhook
	var i int
	for msg := range s.Messages() {
		i++

		err = json.Unmarshal(msg.Value, &hook)
		if err != nil {
			// not a webhook event
			fmt.Println(i, string(msg.Value))
			continue
		}

		fmt.Printf("%d %+v\n", i, hook)
	}

}
