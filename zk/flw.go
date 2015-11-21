package zk

import (
	"time"
)

func (this *ZkZone) runZkFourLetterCommand(cmd string) map[string]string {
	servers := this.conf.ZkServers()
	r := make(map[string]string, len(servers))
	for _, server := range servers {
		b, err := zkFourLetterWord(server, cmd, time.Minute)
		if err != nil {
			panic(err)
		}

		r[server] = string(b)
	}

	return r
}

// Lists detailed information on watches for the server, by session.
func (this *ZkZone) ZkWatchers() map[string]string {
	return this.runZkFourLetterCommand("wchp")
}

func (this *ZkZone) ZkDump() map[string]string {
	return this.runZkFourLetterCommand("dump")
}

// Lists full details for the server.
func (this *ZkZone) ZkStats() map[string]string {
	return this.runZkFourLetterCommand("stat") // srvr
}

// Print details about serving configuration.
func (this *ZkZone) ZkConf() map[string]string {
	return this.runZkFourLetterCommand("conf")
}

// List full connection/session details for all clients connected to this server.
func (this *ZkZone) ZkConnections() map[string]string {
	return this.runZkFourLetterCommand("cons")
}

// Print details about serving environment
func (this *ZkZone) ZkEnv() map[string]string {
	return this.runZkFourLetterCommand("envi")
}
