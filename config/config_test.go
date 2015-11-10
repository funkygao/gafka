package config

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestLoadConfig(t *testing.T) {
	cf := LoadConfig("../etc/gafka.cf")
	t.Logf("%+v", cf)
	assert.Equal(t, 3, len(cf.Zones))

}
