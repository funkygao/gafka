package disk

import (
	"os"

	gio "github.com/funkygao/golib/io"
)

func mkdirIfNotExist(dir string) (err error) {
	if gio.DirExists(dir) {
		return
	}

	err = os.MkdirAll(dir, 0700)
	return
}
