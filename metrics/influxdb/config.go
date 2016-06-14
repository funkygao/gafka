package influxdb

import (
	"net/url"
	"time"

	"github.com/funkygao/gafka/ctx"
)

type config struct {
	interval time.Duration
	hostname string // local host name

	url      url.URL
	database string // influxdb database
	username string // influxdb username
	password string
}

func NewConfig(uri, db, user, pass string, interval time.Duration) (*config, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	return &config{
		hostname: ctx.Hostname(),
		url:      *u,
		database: db,
		username: user,
		password: pass,
		interval: interval,
	}, nil
}
