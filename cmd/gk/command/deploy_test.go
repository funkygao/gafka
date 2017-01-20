package command

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gocli"
)

func TestValidateLogDirs(t *testing.T) {
	d := Deploy{Ui: &cli.BasicUi{}}
	type fixture struct {
		dirs     string
		expected string
	}
	fixtures := []fixture{
		{"/non-exist/kfk_demo", "/non-exist/kfk_demo"},
		{"/non-exist/kfk_demo/", "/non-exist/kfk_demo/"},
		{"/tmp/kfk_demo", ""},
		{"/tmp/kfk_demo/", ""},
		{"/kfk_demo1", ""},
		{"/kfk_demo1/", ""},
	}
	for _, f := range fixtures {
		assert.Equal(t, f.expected, d.validateLogDirs(f.dirs))
	}

}
