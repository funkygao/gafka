package disk

import (
	"fmt"
	"os"
	"testing"

	"github.com/funkygao/assert"
)

func TestConfigValidate(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotEqual(t, nil, cfg.Validate())
}

func TestServiceNextBaseDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Dirs = []string{"a", "b", "c"}
	defer func() {
		for _, dir := range cfg.Dirs {
			os.RemoveAll(dir)
		}
	}()

	s := New(cfg)
	assert.Equal(t, nil, s.Start())
	s.Append("c1", "t1", []byte("key"), []byte("value"))
	for i := 0; i < 10; i++ {
		s.Append(fmt.Sprintf("c%d", i), "t1", []byte("key"), []byte("value"))

		t.Logf("next dir: %s", s.nextBaseDir())
	}
}
