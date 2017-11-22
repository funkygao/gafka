package monitor

import (
	"encoding/json"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ZKMember struct {
	Id     int    `json:"id"`
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	DNS    string `json:"dns"`
	Role   string `json:"role"`
	Online bool   `json:"online"`
}

type ZKCluster struct {
	All []*ZKMember `json:"members"`
}

// zkFourLetterWord execute ZooKeeper Commands: The Four Letter Words
// conf, cons, crst, envi, ruok, stat, wchs, wchp
func zkFourLetterWord(server, command string, timeout time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", server, timeout)

	if err != nil {
		return nil, err
	}

	// the zookeeper server should automatically close this socket
	// once the command has been processed, but better safe than sorry
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(timeout))

	_, err = conn.Write([]byte(command))
	if err != nil {
		return nil, err
	}

	conn.SetReadDeadline(time.Now().Add(timeout))

	resp, err := ioutil.ReadAll(conn)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func zkMemRole(server string) (role string, err error) {

	// using mntr to find zk_server_state
	resp, err := zkFourLetterWord(server, "mntr", time.Second*30)
	if err != nil {
		return
	}

	lines := string(resp)
	parts := strings.Split(lines, "\n")
	roleTag := "zk_server_state"
	for _, l := range parts {
		if strings.HasPrefix(l, roleTag) {
			role = strings.TrimSpace(l[len(roleTag):])
			return
		}
	}

	return
}

// @rest GET /zk/cluster
// get zookeeper cluster info
// eg: curl -XGET http://192.168.149.150:10025/zk/cluster
// TODO authz and rate limitation
func (this *Monitor) zkClusterHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	var err error
	var zkCluster ZKCluster
	servers := this.zkzone.ZkAddrList()
	sort.Strings(servers)
	for id, s := range servers {

		var zkMem = &ZKMember{}
		zkCluster.All = append(zkCluster.All, zkMem)

		// get ip, port info
		info := strings.Split(s, ":")
		if len(info) == 2 {
			zkMem.IP = info[0]
			port, err := strconv.Atoi(info[1])
			if err != nil {
				zkMem.Port = -1
			} else {
				zkMem.Port = port
			}
		} else {
			zkMem.IP = s
			zkMem.Port = -1
		}

		// get dns info
		if dns, present := ctx.ReverseDnsLookup(zkMem.IP, zkMem.Port); present {
			zkMem.DNS = dns
		}

		// get zk id info
		zkMem.Id = id

		// get zk role info
		zkMem.Role, err = zkMemRole(s)
		if err != nil {
			continue
		}

		// get all info, then online
		zkMem.Online = true
	}

	b, _ := json.Marshal(zkCluster)
	w.Write(b)

	return
}
