package meta

import (
	"fmt"
)

func KafkaTopic(appid string, topic string, ver string) string {
	return fmt.Sprintf("%s.%s.%s", appid, topic, ver)
}
