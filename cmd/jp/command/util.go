package command

import (
	"os/user"
	"path/filepath"
)

func jsfFile() string {
	u, err := user.Current()
	swallow(err)
	return filepath.Join(u.HomeDir, ".jsf")
}

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}
