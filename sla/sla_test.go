package sla

import (
	"strings"
	"testing"

	"github.com/funkygao/assert"
)

func TestDefaultSla(t *testing.T) {
	sla := DefaultSla()
	assert.Equal(t, true, sla.IsDefault())
	sla.Partitions = 2
	assert.Equal(t, false, sla.IsDefault())
}

func TestSlaDump(t *testing.T) {
	sla := DefaultSla()
	sla.Replicas = 2 // still the default val
	sla.RetentionHours = 2
	t.Logf("%v", sla.DumpForTopicsCli())
	assert.Equal(t, "--config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))
	sla.Replicas = 3
	assert.Equal(t, "--replication-factor 3 --config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))

	sla.Partitions = -1 // invalid setter
	assert.Equal(t, "--replication-factor 3 --config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))
}

func TestSlaRententionHoursFloat(t *testing.T) {
	sla := DefaultSla()
	assert.Equal(t, nil, sla.ParseRetentionHours("3"))
	sla.ParseRetentionHours("72")
	assert.Equal(t, true, sla.IsDefault())

	sla.ParseRetentionHours("0.5")
	t.Logf("%v", sla.DumpForTopicsCli())
}
