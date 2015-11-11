package config

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestLoadConfig(t *testing.T) {
	LoadConfig("../etc/gafka.cf")
	t.Logf("%+v", conf)
	assert.Equal(t, 3, len(conf.zones))
	assert.Equal(t, "debug", conf.logLevel)

}
