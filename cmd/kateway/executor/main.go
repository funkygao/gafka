package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/api"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/ctx"
)

var (
	addr string
	zone string
	cf   string
)

func init() {
	flag.StringVar(&addr, "addr", "http://localhost:9192", "sub kateway addr")
	flag.StringVar(&zone, "z", "", "zone name")
	flag.StringVar(&cf, "cf", "/etc/kateway.cf", "config file")
	flag.Parse()

	if zone == "" {
		panic("-z required")
	}

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// gk topics -z local -c me -add _executor._kateway.v1 -replicas 1
// curl -i -XPOST -H "Pubkey: mypubkey" -d '{"cmd":"createTopic", "topic": "hello", "appid": "xx", "ver": "v1"}' "http://localhost:9191/topics/v1/_kateway"

func main() {
	ctx.LoadConfig(cf)

	metaConf := zkmeta.DefaultConfig(zone)
	metaConf.Refresh = time.Hour
	meta.Default = zkmeta.New(metaConf)
	meta.Default.Start()

	cf := api.DefaultConfig()
	cf.Debug = true
	c := api.NewClient("_executor", cf)
	c.Connect(addr)
	for {
		err := c.Subscribe("_executor", "_kateway", "v1", "_addtopic",
			func(statusCode int, cmd []byte) (err error) {
				if statusCode != http.StatusOK {
					log.Printf("err[%d] backoff 10s: %s", statusCode, string(cmd))
					time.Sleep(time.Second * 10)
					return nil
				}

				v := make(map[string]interface{})
				err = json.Unmarshal(cmd, &v)
				if err != nil {
					log.Printf("%s: %v", string(cmd), err)
					return nil
				}

				topic := v["topic"].(string)
				appid := v["appid"].(string)
				ver := v["ver"].(string)
				topic = meta.KafkaTopic(appid, topic, ver)
				var lines []string
				cluster := meta.Default.LookupCluster(appid, topic)
				lines, err = meta.Default.ZkCluster(cluster).AddTopic(topic, 1, 1) // TODO
				if err != nil {
					log.Printf("add: %v", err)
					time.Sleep(time.Second * 10)
					return nil
				}

				for _, l := range lines {
					log.Printf("add topic[%s]: %s", topic, l)
				}

				return nil
			})

		log.Printf("backoff 10s for: %s", err)
		time.Sleep(time.Second * 10)
	}

}
