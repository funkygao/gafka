package ctx

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strings"
)

func Hostname() string {
	ensureLogLoaded()
	return conf.hostname
}

func LogLevel() string {
	ensureLogLoaded()
	return conf.logLevel
}

func Zones() map[string]string {
	ensureLogLoaded()
	return conf.zones
}

func ZkDefaultZone() string {
	ensureLogLoaded()
	return conf.zkDefaultZone
}

func Tunnels() map[string]string {
	ensureLogLoaded()
	return conf.tunnels
}

func ConsulBootstrap() string {
	ensureLogLoaded()
	return conf.consulBootstrap
}

func ConsulBootstrapList() []string {
	return strings.Split(ConsulBootstrap(), ",")
}

func KafkaHome() string {
	ensureLogLoaded()
	return conf.kafkaHome
}

func InfluxdbHost() string {
	ensureLogLoaded()
	return conf.influxdbHost
}

func SortedZones() []string {
	ensureLogLoaded()
	return conf.sortedZones()
}

func Alias(cmd string) (alias string, present bool) {
	alias, present = conf.aliases[cmd]
	return
}

func ZoneZkAddrs(zone string) (zkAddrs string) {
	ensureLogLoaded()
	var present bool
	if zkAddrs, present = conf.zones[zone]; present {
		return
	}

	// should never happen
	fmt.Printf("zone[%s] undefined\n", zone)
	os.Exit(1)
	return ""
}

// LocalIP tries to determine a non-loopback address for the local machine
func LocalIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
			if ipnet.IP.To4() != nil || ipnet.IP.To16() != nil {
				return ipnet.IP, nil
			}
		}
	}
	return nil, nil
}

func CurrentUserIsRoot() bool {
	user, err := user.Current()
	if err != nil {
		return false
	}

	return user.Uid == "0"
}
