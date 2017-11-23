package monitor

import (
	"bufio"
	"encoding/json"
	"github.com/funkygao/golib/pipestream"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"strconv"
	"strings"
)

type ConsulNode struct {
	IP      string
	Name    string
	IsAlive bool
}

type Host struct {
	IP     string `json:"ip"`
	Name   string `json:"host"` // host name
	Disk   int    `json:"disk"` // in MB
	Cores  int    `json:"cpu_cores"`
	Memory int    `json:"memory"` // in MB
	ErrMsg string `json:"error"`
}

type Hosts struct {
	All []*Host
}

// @rest POST /kfk/hosts
// get host's info (disk space/MB, cpu cores, memory/MB)
// eg: curl -XPOST http://192.168.149.150:10025/kfk/hosts -d'["192.168.149.150"]'
// TODO authz and rate limitation
func (this *Monitor) hostsHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	type hostsRequest []string

	dec := json.NewDecoder(r.Body)
	var req hostsRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	hosts, err := this.getHosts(req)
	if err != nil {
		log.Error("hosts:%v %v", req, err)
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(hosts)
	w.Write(b)

	return
}

func (this *Monitor) getHosts(reqInfo []string) (hosts Hosts, err error) {
	allNodes, err := getAllNodes()
	if err != nil {
		return
	}

	// all
	if len(reqInfo) == 0 {
		for ip, node := range allNodes {
			h := Host{
				IP:   ip,
				Name: node.Name,
			}

			if !node.IsAlive {
				h.ErrMsg = ErrHostConsulDead.Error()
			}

			hosts.All = append(hosts.All, &h)
		}
	} else {
		for _, ip := range reqInfo {
			h := Host{
				IP: ip,
			}

			if node, ok := allNodes[ip]; ok {
				h.Name = node.Name
				if !node.IsAlive {
					h.ErrMsg = ErrHostConsulDead.Error()
				}
			} else {
				h.ErrMsg = ErrHostNotExist.Error()
			}

			hosts.All = append(hosts.All, &h)
		}
	}

	for _, h := range hosts.All {
		if h.ErrMsg == "" {
			this.getHost(h)
		}
	}
	return
}

func (this *Monitor) getHost(host *Host) {

	var err error
	defer func() {
		if err != nil {
			host.ErrMsg = err.Error()
		}
	}()

	host.Disk, err = getNodeDisk(host.Name)
	if err != nil {
		return
	}

	host.Cores, err = getNodeCores(host.Name)
	if err != nil {
		return
	}

	host.Memory, err = getNodeMemory(host.Name)
	if err != nil {
		return
	}

	return
}

// get nodes ip map (ip --> node_name)
func getAllNodes() (nodes map[string]*ConsulNode, err error) {
	nodes = make(map[string]*ConsulNode)

	cmd := pipestream.New("consul", "members")
	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	//[root@MyCentOS7VM ~]# consul members
	//Node         Address               Status  Type    Build  Protocol  DC
	//MyCentOS7VM  192.168.149.150:8301  alive   server  0.7.1  2         my_lab

	lineCnt := 0
	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		lineCnt++
		// ignore title
		if lineCnt == 1 {
			continue
		}

		node, e := extractNode(line)
		if e != nil {
			log.Error("node line:%s %v", line, err)
			continue
		}

		nodes[node.IP] = &node
	}

	return
}

//MyCentOS7VM  192.168.149.150:8301  alive   server  0.7.1  2         my_lab
func extractNode(line string) (node ConsulNode, err error) {
	parts := strings.Fields(line)
	if len(parts) != 7 {
		err = ErrInvalidConsulLine
		return
	}

	node.Name = parts[0]
	addr := parts[1]
	info := strings.Split(addr, ":")
	if len(info) == 2 {
		node.IP = info[0]
	} else {
		node.IP = addr
	}

	if parts[2] == "alive" {
		node.IsAlive = true
	}

	return
}

// Get node's DiskSize in MB
func getNodeDisk(node string) (diskSize int, err error) {

	cmd := pipestream.New("consul", "exec", "-node", node,
		"df", "-m", "|", "grep -v 1M-blocks")

	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	//[root@MyCentOS7VM ~]# consul exec -node="MyCentOS7VM" "df -m | grep -v "1M-blocks""
	//	MyCentOS7VM: Filesystem              1M-blocks  Used Available Use% Mounted on
	//	MyCentOS7VM: /dev/mapper/centos-root     51175  9226     41950  19% /
	//	MyCentOS7VM: devtmpfs                     3889     0      3889   0% /dev
	//	MyCentOS7VM: tmpfs                        3905     1      3905   1% /dev/shm
	//	MyCentOS7VM: tmpfs                        3905    10      3896   1% /run
	//	MyCentOS7VM: tmpfs                        3905     0      3905   0% /sys/fs/cgroup
	//	MyCentOS7VM: /dev/sda1                     497   158       340  32% /boot
	//	MyCentOS7VM: /dev/mapper/centos-home    144898 29920    114978  21% /home
	//	MyCentOS7VM: tmpfs                         781     1       781   1% /run/user/987
	//	MyCentOS7VM: tmpfs                         781     1       781   1% /run/user/1000
	//	MyCentOS7VM:
	//==> MyCentOS7VM: finished with exit code 0
	//1 / 1 node(s) completed / acknowledged

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()

		// ignore 2 bottom lines
		if strings.Contains(line, "finished") || strings.Contains(line, "acknowledged") {
			continue
		}

		d, e := extractDiskSize(line)
		if e != nil {
			log.Error("diskSize line:%s %v", line, err)
			continue
		}

		diskSize += d
	}

	return
}

// MyCentOS7VM: /dev/mapper/centos-root     51175  9226     41950  19% /
func extractDiskSize(line string) (diskSize int, err error) {
	parts := strings.Fields(line)
	if len(parts) != 7 {
		return
	}

	diskSize, err = strconv.Atoi(parts[2])
	return

}

// Get Node Cores
func getNodeCores(node string) (cores int, err error) {

	// consul exec -node="MyCentOS7VM" "cat /proc/cpuinfo | grep processor  | wc -l"
	cmd := pipestream.New("consul", "exec", "-node", node,
		"cat", "/proc/cpuinfo", "|", "grep processor", "|", "wc -l")
	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	//[root@MyCentOS7VM ~]# consul exec -node="MyCentOS7VM" "cat /proc/cpuinfo | grep processor  | wc -l"
	//	MyCentOS7VM: 4
	//	MyCentOS7VM:
	//==> MyCentOS7VM: finished with exit code 0
	//1 / 1 node(s) completed / acknowledged

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()

		// ignore 2 bottom lines
		if strings.Contains(line, "finished") || strings.Contains(line, "acknowledged") {
			continue
		}

		c, e := extractCores(line)
		if e != nil {
			log.Error("cores line:%s %v", line, err)
			continue
		}

		cores += c
	}

	return
}

// MyCentOS7VM: 4
func extractCores(line string) (cores int, err error) {
	parts := strings.Fields(line)
	if len(parts) != 2 {
		return
	}

	cores, err = strconv.Atoi(parts[1])
	return
}

// Get Node's MemSize in MB
func getNodeMemory(node string) (memSize int, err error) {

	// consul exec -node="MyCentOS7VM" "cat /proc/cpuinfo | grep processor  | wc -l"
	cmd := pipestream.New("consul", "exec", "-node", node,
		"cat", "/proc/meminfo", "|", "grep MemTotal")
	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	//[root@MyCentOS7VM ~]# consul exec -node="MyCentOS7VM" "cat /proc/meminfo | grep MemTotal"
	//	MyCentOS7VM: MemTotal:        7995716 kB
	//	MyCentOS7VM:
	//==> MyCentOS7VM: finished with exit code 0
	//1 / 1 node(s) completed / acknowledged

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()

		// ignore 2 bottom lines
		if strings.Contains(line, "finished") || strings.Contains(line, "acknowledged") {
			continue
		}

		m, e := extractMemorySize(line)
		if e != nil {
			log.Error("memSize line:%s %v", line, err)
			continue
		}

		memSize += m
	}

	return
}

//	MyCentOS7VM: MemTotal:        7995716 kB
func extractMemorySize(line string) (memSize int, err error) {
	parts := strings.Fields(line)
	if len(parts) != 4 {
		return
	}

	m, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}

	memSize = int(m / 1024) // KB --> MB
	return
}
