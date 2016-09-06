package disk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestBlockBasic(t *testing.T) {
	b := block{
		magic: currentMagic,
		key:   []byte("abc"),
		value: []byte("12345678"),
	}

	assert.Equal(t, uint32(3), b.keyLen())
	assert.Equal(t, uint32(8), b.valueLen())
}

func TestBlockReadWrite(t *testing.T) {
	t.SkipNow()
}
