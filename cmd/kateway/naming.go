package main

import (
	"fmt"
)

func kafkaTopic(appid string, topic string, ver string) string {
	return fmt.Sprintf("%s.%s.%s", appid, topic, ver)
}
