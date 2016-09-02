package disk

import (
	"os"
	"testing"

	"github.com/funkygao/assert"
)

func TestMkdirIfNotExist(t *testing.T) {
	dir := "xxx"
	defer os.Remove(dir)

	assert.Equal(t, nil, mkdirIfNotExist(dir))
	assert.Equal(t, nil, mkdirIfNotExist(dir))
}
