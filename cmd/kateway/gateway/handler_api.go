package gateway

import (
	"net/http"
	"net/url"

	"github.com/influxdata/influxdb/client"
	"github.com/julienschmidt/httprouter"
)

// TODO
func (this *Gateway) appMetricsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	myAppid := r.Header.Get(HttpHeaderAppid) // TODO auth
	if myAppid == "" {

	}

	u, _ := url.Parse(Options.InfluxServer)
	conn, err := client.NewClient(client.Config{
		URL: *u,
	})
	if err != nil {
		return
	}
	conn.Query(client.Query{
		Command:  "",
		Database: Options.InfluxDbName,
	})

}
