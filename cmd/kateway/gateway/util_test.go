package gateway

import (
	"testing"
	"time"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

func TestIsBrokerError(t *testing.T) {
	assert.Equal(t, false, isBrokerError(store.ErrRebalancing))
	assert.Equal(t, false, isBrokerError(store.ErrTooManyConsumers))
}

func TestTimeParseDuration(t *testing.T) {
	_, err := time.ParseDuration("")
	assert.Equal(t, "time: invalid duration ", err.Error())

	d, err := time.ParseDuration("-5s")
	assert.Equal(t, -5., d.Seconds())
	assert.Equal(t, nil, err)

	d, err = time.ParseDuration("5s")
	assert.Equal(t, 5., d.Seconds())
	assert.Equal(t, nil, err)

	d, err = time.ParseDuration("100ms")
	assert.Equal(t, nil, err)
	assert.Equal(t, int64(1000*1000*100), d.Nanoseconds())
}

func TestGetHttpRemoteIp(t *testing.T) {
	req, err := mockHttpRequest()
	if err != nil {
		t.Fatal(err)
	}

	req.RemoteAddr = "10.209.18.15:42395"
	assert.Equal(t, "1.1.1.12", getHttpRemoteIp(req))

	req.Header.Del(HttpHeaderXForwardedFor)
	assert.Equal(t, "10.209.18.15", getHttpRemoteIp(req))
}

// 73.4 ns/op
func BenchmarkGetHttpRemoteIp(b *testing.B) {
	r, err := mockHttpRequest()
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		getHttpRemoteIp(r)
	}
}
