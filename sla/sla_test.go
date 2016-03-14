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

func TestSlaDumpForCreateTopic(t *testing.T) {
	sla := DefaultSla()
	sla.Replicas = 2 // still the default val
	sla.RetentionHours = 2
	t.Logf("%v", sla.DumpForCreateTopic())
	assert.Equal(t, "--partitions 1 --replication-factor 2", strings.Join(sla.DumpForCreateTopic(), " "))
	sla.Replicas = 3
	assert.Equal(t, "--partitions 1 --replication-factor 3", strings.Join(sla.DumpForCreateTopic(), " "))

	sla.Partitions = -1 // invalid setter
	assert.Equal(t, "--partitions 1 --replication-factor 3", strings.Join(sla.DumpForCreateTopic(), " "))
}

func TestSlaDumpForAlterTopic(t *testing.T) {
	sla := DefaultSla()
	sla.Replicas = 3
	sla.RetentionHours = 2
	assert.Equal(t, "--config retention.ms=7200000", strings.Join(sla.DumpForAlterTopic(), " "))
	sla.RetentionHours = -1 // invalid setter, restore to default
	assert.Equal(t, 0, len(sla.DumpForAlterTopic()), " ")
	sla.RetentionBytes = 10 << 20
	assert.Equal(t, "--config retention.bytes=10485760", strings.Join(sla.DumpForAlterTopic(), " "))
}

func TestSlaRententionHoursFloat(t *testing.T) {
	sla := DefaultSla()
	assert.Equal(t, nil, sla.ParseRetentionHours("3"))
	sla.ParseRetentionHours("168")
	assert.Equal(t, true, sla.IsDefault())

	sla.ParseRetentionHours("0.5")
	t.Logf("%v", sla.DumpForCreateTopic())

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
