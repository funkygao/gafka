package influxdb

import (
	"net/url"
	"strings"
)

const (
	tagSep = "#"
)

// name: appid=5&topic=a.b.c&ver=v1#pub.qps
// TODO deprecated
func (this *runner) extractTagsFromMetricsName(name string) (realName string, tags map[string]string) {
	tags = map[string]string{
		"host": this.cf.hostname,
	}

	i := strings.Index(name, tagSep)
	if i < 0 {
		// no tag
		return name, tags
	}

	// has tag
	realName = name[i+1:]

	u, _ := url.ParseQuery(name[:i])
	for k, v := range u {
		tags[k] = v[0] // we use only 1st item
	}

	return
}
