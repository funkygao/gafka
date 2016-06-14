package influxdb

import (
	"strings"
)

func extractFromMetricsName(name string) (appid, topic, ver, realname string) {
	if name[0] != '{' {
		realname = name
		return
	}

	i := strings.Index(name, "}")
	realname = name[i+1:]
	p := strings.SplitN(name[1:i], ".", 3)
	appid, topic, ver = p[0], p[1], p[2]
	return
}
