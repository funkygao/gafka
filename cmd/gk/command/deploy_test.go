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
		fixture{"/non-exist/kfk_demo", "/non-exist/kfk_demo"},
		fixture{"/non-exist/kfk_demo/", "/non-exist/kfk_demo/"},
		fixture{"/tmp/kfk_demo", ""},
		fixture{"/tmp/kfk_demo/", ""},
		fixture{"/kfk_demo1", ""},
		fixture{"/kfk_demo1/", ""},
	}
	for _, f := range fixtures {
		assert.Equal(t, f.expected, d.validateLogDirs(f.dirs))
	}

}
