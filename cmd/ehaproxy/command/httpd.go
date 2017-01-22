package command

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/ctx"
	log "github.com/funkygao/log4go"
)

func (this *Start) runMonitorServer(addr string) {
	http.HandleFunc("/v1/ver", this.versionHandler)
	http.HandleFunc("/v1/status", this.statusHandler)
	http.HandleFunc("/alive", this.aliveHandler)

	log.Info("status web server on %s ready", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Error("status web server: %s", err)
	}
}

var cols = []string{
	"# pxname", // proxy name
	"svname",   // service name
	"scur",     // current sessions
	//"smax",     // max sessions
	"stot", // total sessions
	"bin",  // bytes in
	"bout", // bytes out
	//"dreq",  // denied requests
	//"dresp", // denied response
	//"ereq",     // request errors
	"econ",   // connection errors
	"wredis", // redispatches (warning)
	//"rate",     // number of sessions per second over last elapsed second
	"rate_max", // max number of new sessions per second
	"hrsp_1xx", // http responses with 1xx code
	//"hrsp_2xx",
	//"hrsp_3xx",
	"hrsp_4xx",
	"hrsp_5xx",
	"cli_abrt", // number of data transfers aborted by the client
	"srv_abrt", // number of data transfers aborted by the server (inc. in eresp)
}

var colsMap = make(map[string]struct{})

func init() {
	for _, c := range cols {
		colsMap[c] = struct{}{}
	}
}

// TODO auth
func (this *Start) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf8")
	w.Header().Set("Server", "ehaproxy")

	log.Info("%s status", r.RemoteAddr)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		aggStats = make(map[string]map[string]int64)
	)
	for i := 0; i < ctx.NumCPU(); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			uri := fmt.Sprintf("http://127.0.0.1:%d/stats?stats;csv", i+dashboardPortHead)
			stats := fetchDashboardStats(uri)
			mu.Lock()
			for name, colVals := range stats {
				if _, present := aggStats[name]; !present {
					aggStats[name] = make(map[string]int64)
				}
				for k, v := range colVals {
					aggStats[name][k] += v
				}
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	b, _ := json.Marshal(aggStats)
	w.Write(b)
}

func fetchDashboardStats(statsUri string) (v map[string]map[string]int64) {
	v = make(map[string]map[string]int64)

	client := http.Client{Timeout: time.Second * 4}
	resp, err := client.Get(statsUri)
	if err != nil {
		log.Error("%s: %v", statsUri, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Error("%s: got status %v", statsUri, resp.Status)
		return
	}

	reader := csv.NewReader(resp.Body)
	records, err := reader.ReadAll()
	if err != nil {
		log.Error("%s: %v", statsUri, err)
		return
	}

	theCols := make(map[int]string) // col:name
	for i, row := range records {
		if i == 0 {
			// header
			for j, col := range row {
				theCols[j] = col
			}
			continue
		}

		if row[1] != "BACKEND" || (row[0] != "pub" && row[0] != "sub" && row[0] != "man") {
			continue
		}

		v[row[0]] = make(map[string]int64)
		for i, col := range row {
			if _, present := colsMap[theCols[i]]; !present {
				// ignore unwanted metrics
				continue
			}
			if strings.HasPrefix(theCols[i], "#") || theCols[i] == "svname" {
				continue
			}

			n, _ := strconv.ParseInt(col, 10, 64)
			v[row[0]][theCols[i]] = n
		}
	}

	return
}

func (this *Start) versionHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf8")
	w.Header().Set("Server", "ehaproxy")

	log.Info("%s ver", r.RemoteAddr)

	v := map[string]string{
		"version": gafka.BuildId,
		"uptime":  strconv.FormatInt(this.startedAt.Unix(), 10),
	}
	b, _ := json.Marshal(v)
	w.Write(b)
}

func (this *Start) aliveHandler(w http.ResponseWriter, r *http.Request) {
	if this.quiting.Get() {
		w.WriteHeader(http.StatusSeeOther)

		log.Info("offloaded from %s for %s%s", r.RemoteAddr, r.Host, r.RequestURI)
		if this.deadN.Add(1) == 3 {
			// TODO more strict check rule: check each port
			log.Info("enough death reported, safe to shutdown")
			close(this.safeShutdown)
		}
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("alive"))
	}
}
