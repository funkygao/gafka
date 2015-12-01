package zk

import (
	"fmt"
	"time"
)

// Returns {zkHost: outputLines}
func (this *ZkZone) RunZkFourLetterCommand(cmd string) map[string]string {
	servers := this.conf.ZkServers()
	r := make(map[string]string, len(servers))
	for _, server := range servers {
		b, err := zkFourLetterWord(server, cmd, time.Minute)
		if err != nil {
			fmt.Println(err)
		}

		r[server] = string(b)
	}

	return r
}
