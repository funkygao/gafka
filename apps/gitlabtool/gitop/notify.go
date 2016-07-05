// +build darwin
package main

import (
	"github.com/ciarand/notify"
)

func displayNotify(txt string, sound string) {
	n := notify.NewNotificationWithSound("gitop", txt, sound)
	n.Display()
}
