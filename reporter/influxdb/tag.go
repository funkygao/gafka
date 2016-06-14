package influxdb

import (
	"strings"
)

func (this *reporter) extractTagsFromMetricsName(name string) (tags map[string]string) {
	i := strings.Index(name, "}")
	//realname := name[i+1:]
	p := strings.SplitN(name[1:i], ".", 3)
	appid, topic, ver := p[0], p[1], p[2]

	if appid == "" {
		tags = map[string]string{
			"host": this.cf.hostname,
		}
	} else {
		tags = map[string]string{
			"host":  this.cf.hostname,
			"appid": appid,
			"topic": topic,
			"ver":   ver,
		}
	}

	return
}
