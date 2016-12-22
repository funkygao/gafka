package influxquery

import (
	"errors"

	"github.com/influxdata/influxdb/client/v2"
)

var (
	errInfluxResult = errors.New("bad reply from influx query result")
	influxClient    client.Client
)

func queryInfluxDB(addr, db, cmd string) (res []client.Result, err error) {
	// FIXME not atomic
	if influxClient == nil {
		influxClient, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     addr,
			Username: "",
			Password: "",
		})
		if err != nil {
			return
		}
	}

	if response, err := influxClient.Query(client.Query{
		Command:  cmd,
		Database: db,
	}); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}

	return res, nil
}
