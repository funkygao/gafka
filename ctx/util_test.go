package ctx

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestExtractLoadAvg1m(t *testing.T) {
	line := "    hostname.foo.bar:  12:19:20 up 144 days,  3:21,  0 users,  load average: 0.10, 0.18, 0.67"
	avg1m, err := ExtractLoadAvg1m(line)
	assert.Equal(t, 0.1, avg1m)
	assert.Equal(t, nil, err)

	line = "adsfasf asdfasdf"
	avg1m, err = ExtractLoadAvg1m(line)
	assert.Equal(t, errInvalidLoadAvg, err)

	line = "hostname.foo.bar:  12:19:20 up 144 days,  3:21,  0 users,  load average: 1.10, 0.18, 0.67"
	avg1m, err = ExtractLoadAvg1m(line)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1.1, avg1m)

	line = "hostname.foo.bar:  12:19:20 up 144 days,  3:21,  0 users,  load average: 12.10, 0.18, 0.67"
	avg1m, err = ExtractLoadAvg1m(line)
	assert.Equal(t, nil, err)
	assert.Equal(t, 12.1, avg1m)
}
