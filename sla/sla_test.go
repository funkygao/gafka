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
	assert.Equal(t, "--partitions 1 --config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))
	sla.Replicas = 3
	assert.Equal(t, "--partitions 1 --replication-factor 3 --config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))

	sla.Partitions = -1 // invalid setter
	assert.Equal(t, "--partitions 1 --replication-factor 3 --config retention.ms=7200000", strings.Join(sla.DumpForTopicsCli(), " "))
}

func TestSlaRententionHoursFloat(t *testing.T) {
	sla := DefaultSla()
	assert.Equal(t, nil, sla.ParseRetentionHours("3"))
	sla.ParseRetentionHours("168")
	assert.Equal(t, true, sla.IsDefault())

	sla.ParseRetentionHours("0.5")
	t.Logf("%v", sla.DumpForTopicsCli())

	assert.Equal(t, ErrNotNumber, sla.ParseRetentionHours("abc"))
	assert.Equal(t, ErrEmptyArg, sla.ParseRetentionHours(""))
	assert.Equal(t, ErrNotNumber, sla.ParseRetentionHours(" "))
	assert.Equal(t, ErrNegative, sla.ParseRetentionHours("-9"))
}

func TestValidateGuardName(t *testing.T) {
	assert.Equal(t, true, ValidateGuardName("dead"))
	assert.Equal(t, true, ValidateGuardName("retry"))
	assert.Equal(t, false, ValidateGuardName(""))
	assert.Equal(t, false, ValidateGuardName("foo"))
}
