// exaplains zookeeper session expires mechanism.
// The code is extracted from zookeeper server code.
package main

import (
	"flag"
	"fmt"
)

var (
	tickTime          = 3000 // zoo.cfg, default 3s
	minSessionTimeout = -1   // zoo.cfg
	maxSessionTimeout = -1   // zoo.cfg
)

func getMinSessionTimeout() int {
	if minSessionTimeout == -1 {
		return tickTime * 2
	} else {
		return minSessionTimeout
	}
}

func getMaxSessionTimeout() int {
	if maxSessionTimeout == -1 {
		return tickTime * 20
	} else {
		return maxSessionTimeout
	}
}

func getRealSessionTimeout(sessionTimeout int) int {
	min := getMinSessionTimeout()
	if sessionTimeout < min {
		sessionTimeout = min
	}
	max := getMaxSessionTimeout()
	if sessionTimeout > max {
		sessionTimeout = max
	}
	return sessionTimeout
}

func main() {
	var clientConfiggedSessionTimeout int
	flag.IntVar(&clientConfiggedSessionTimeout, "c", 30*1000, "client side zk session timeout, which will be sent to zk for negotiation")
	flag.IntVar(&tickTime, "t", 3000, "tickTime in zoo.cfg")
	flag.Parse()

	fmt.Printf("default between: %ds ~ %ds\n",
		getRealSessionTimeout(-1)/1000,
		getRealSessionTimeout(1<<30)/1000)

	fmt.Printf("client sent:%ds => got %ds\n",
		clientConfiggedSessionTimeout/1000,
		getRealSessionTimeout(clientConfiggedSessionTimeout)/1000)
}
