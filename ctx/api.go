package ctx

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
)

func NamedZoneZkAddrs(zone string) string {
	parts := strings.Split(ZoneZkAddrs(zone), ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		ip, port, err := net.SplitHostPort(p)
		if err != nil {
			fmt.Printf("zone[%s]: %s\n", zone, err)
			os.Exit(1)
		}

		host, present := ReverseDnsLookup(ip, 0)
		if present {
			ip = host
		}

		addrs = append(addrs, net.JoinHostPort(ip, port))
	}
	return strings.Join(addrs, ",")
}

func ZoneZkAddrs(zone string) (zkAddrs string) {
	ensureLogLoaded()

	if z, present := conf.zones[zone]; present {
		return z.Zk
	}

	// should never happen
	fmt.Printf("zone[%s] undefined\n", zone)
	os.Exit(1)
	return ""
}

func Zones() map[string]string {
	ensureLogLoaded()

	r := make(map[string]string, len(conf.zones))
	for name, z := range conf.zones {
		r[name] = z.Zk
	}
	return r
}

func Zone(z string) *zone {
	ensureLogLoaded()

	return conf.zones[z]
}

func ZkDefaultZone() string {
	ensureLogLoaded()
	return conf.zkDefaultZone
}

func DefaultZone() string {
	ensureLogLoaded()
	return conf.zkDefaultZone
}

func Hostname() string {
	ensureLogLoaded()
	return conf.hostname
}

func LogLevel() string {
	ensureLogLoaded()
	return conf.logLevel
}

// UpgradeCenter return the uri where to fetch gk/kguard/kateway/.gafka.cf files.
func UpgradeCenter() string {
	ensureLogLoaded()
	fromEnv := os.Getenv("UPGRADE_CENTER")
	if fromEnv != "" {
		return fromEnv
	}

	return conf.upgradeCenter
}

func ReverseDnsLookup(ip string, port int) (string, bool) {
	ensureLogLoaded()
	hosts, present := conf.reverseDns[ip]
	if !present || len(hosts) == 0 {
		return "", false
	}

	if port <= 0 {
		// ignore port as server name
		return hosts[0], present
	}

	// a single host has multiple services each of which has a different port
	// e,g. k[port][a-z].sit.mycorp.kfk.com/kafka  z[port][a-z].sit.mycorp.zk.com/zk
	for _, name := range hosts {
		p := strings.Split(name, ".")
		if strings.Contains(p[0], strconv.Itoa(port)) {
			return name, true
		}
	}

	return "", false
}

func KafkaHome() string {
	ensureLogLoaded()
	return conf.kafkaHome
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

func AliasesWithValue() map[string]string {
	return conf.aliases
}

// LocalIP tries to determine a non-loopback address for the local machine
func LocalIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP, nil
			}
		}
	}
	return nil, nil
}

func NumCPU() int {
	return runtime.NumCPU()
}

func NumCPUStr() string {
	return strconv.Itoa(NumCPU())
}

func CurrentUserIsRoot() bool {
	user, err := user.Current()
	if err != nil {
		return false
	}

	return user.Uid == "0"
}
