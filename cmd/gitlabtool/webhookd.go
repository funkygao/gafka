// webhookd is a webhook endpoint that is integrated with gitlab.
// it accepts events from gitlab hook and send it to pubsub system.
package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/funkygao/gafka/cmd/kateway/api/v1"
)

var (
	client *api.Client
)

func main() {
	cf := api.DefaultConfig("30", "32f02594f55743eeb1efcf75db6dd8a0")
	cf.Pub.Endpoint = "pub.intra.mycorp.com"
	client = api.NewClient(cf)

	http.HandleFunc("/", handle)
	log.Fatal(http.ListenAndServe(":9199", nil))
}

func handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("%s %s %s", r.Method, r.RequestURI, string(body))

	var opt api.PubOption
	opt.Topic = "gitlab_events"
	opt.Ver = "v1"
	if err = client.Pub("", body, opt); err != nil {
		log.Println(err)
	}
}
