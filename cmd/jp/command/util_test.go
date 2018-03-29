package command

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestUmpKey2Interface(t *testing.T) {
	umpKey := "com.jd.eclp.master.shipper.service.impl.ShipServiceImpl.achieveShipList"
	i, m, e := umpkey2interface(umpKey)
	assert.Equal(t, nil, e)
	assert.Equal(t, "com.jd.eclp.master.shipper.service.ShipService", i)
	assert.Equal(t, "achieveShipList", m)
}
