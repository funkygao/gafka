package gateway

import (
	"encoding/json"
	"testing"

	"github.com/funkygao/assert"
)

func TestJsonUnmarshalAckOffsets(t *testing.T) {
	var acks ackOffsets
	s := `
[{"partition":5,"offset":124},{"partition":2,"offset":893}]
	`
	err := json.Unmarshal([]byte(s), &acks)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(acks))
	t.Logf("%#v", acks)
	assert.Equal(t, 2, acks[1].Partition)
	assert.Equal(t, int64(124), acks[0].Offset)
}
