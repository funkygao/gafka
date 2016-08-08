package mysql

import (
	"hash/adler32"
	"strings"
)

const jobTablePrefix = "job_"

// JobTable converts a topic name to a mysql table name.
func JobTable(topic string) string {
	return jobTablePrefix + strings.Replace(topic, ".", "_", -1)
}

// HistoryTable converts a topic name to a mysql history table name.
func HistoryTable(topic string) string {
	return JobTable(topic) + "_archive"
}

// App_id convert a string appid to hash int which is used to locate shard.
func App_id(appid string) int {
	return int(adler32.Checksum([]byte(appid)))
}
