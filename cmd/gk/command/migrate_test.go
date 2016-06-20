package command

import (
	"testing"
)

func TestMigratePartitionNormalize(t *testing.T) {
	m := Migrate{
		partition: "1-5",
	}
	m.normalizePartitions()
	t.Logf("%s", m.partition)
}
