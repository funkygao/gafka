package monitor

import (
	"fmt"
)

var (
	registeredWatchers = make(map[string]func() Watcher)
)

// A Watcher is a plugin of monitor.
type Watcher interface {
	Init(Context)
	Run()
}

func RegisterWatcher(name string, factory func() Watcher) {
	if _, present := registeredWatchers[name]; present {
		panic(fmt.Sprintf("watcher[%s] cannot register twice", name))
	}

	registeredWatchers[name] = factory
}
