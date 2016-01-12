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

func ReverseDnsLookup(ip string) (host string, present bool) {
	ensureLogLoaded()
	host, present = conf.reverseDns[ip]
	return
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

func Aliases() []string {
	r := make([]string, 0, len(conf.aliases))
	for cmd, _ := range conf.aliases {
		r = append(r, cmd)
	}
	return r
}

func NamedZoneZkAddrs(zone string) string {
	parts := strings.Split(ZoneZkAddrs(zone), ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		ip, port, err := net.SplitHostPort(p)
		if err != nil {
			fmt.Printf("zone[%s]: %s\n", zone, err)
			os.Exit(1)
		}

		host, present := ReverseDnsLookup(ip)
		if present {
			ip = host
		}

		addrs = append(addrs, net.JoinHostPort(ip, port))
	}
	return strings.Join(addrs, ",")
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
