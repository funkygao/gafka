package zk

import (
	"strings"
)

type ZkStat struct {
	Version     string
	Latency     string
	Connections string
	Outstanding string
	Mode        string // S=standalone, L=leader, F=follower
	Znodes      string
	Received    string
	Sent        string
}

// Parse `zk stat` output into ZkStat struct.
func ParseStatResult(s string) (stat ZkStat) {
	lines := strings.Split(s, "\n")
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "Zookeeper version:"):
			p := strings.SplitN(l, ":", 2)
			p = strings.SplitN(p[1], ",", 2)
			stat.Version = strings.TrimSpace(p[0])

		case strings.HasPrefix(l, "Latency"):
			stat.Latency = extractStatValue(l)

		case strings.HasPrefix(l, "Sent"):
			stat.Sent = extractStatValue(l)

		case strings.HasPrefix(l, "Received"):
			stat.Received = extractStatValue(l)

		case strings.HasPrefix(l, "Connections"):
			stat.Connections = extractStatValue(l)

		case strings.HasPrefix(l, "Mode"):
			stat.Mode = strings.ToUpper(extractStatValue(l)[:1])

		case strings.HasPrefix(l, "Node count"):
			stat.Znodes = extractStatValue(l)

		case strings.HasPrefix(l, "Outstanding"):
			stat.Outstanding = extractStatValue(l)

		}
	}
	return
}

func extractStatValue(l string) string {
	p := strings.SplitN(l, ":", 2)
	return strings.TrimSpace(p[1])
}
