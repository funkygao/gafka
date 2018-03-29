package command

import (
	"fmt"
	"os/user"
	"path/filepath"
	"strings"
)

func jsfFile(app string) string {
	u, err := user.Current()
	swallow(err)
	return filepath.Join(u.HomeDir, fmt.Sprintf(".%s.jsf", app))
}

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func umpkey2interface(umpKey string) (interfaceName, methodName string, err error) {
	s := strings.Replace(umpKey, ".impl.", ".", -1)
	s = strings.Replace(s, "Impl.", ".", -1)
	tuples := strings.Split(s, ".")
	methodName = tuples[len(tuples)-1]
	interfaceName = strings.Join(tuples[0:len(tuples)-1], ".")
	return
}
