package command

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/gorequest"
)

func refreshScreen() {
	c := exec.Command("clear")
	c.Stdout = os.Stdout
	c.Run()
	c.Wait()
}

func swallow(err error) {
	if err != nil {
		panic(err)
	}
}

func handleCatCommand(ui cli.Ui, zkzone *zk.ZkZone, cluster string, cmd string) {
	if cluster == "" {
		// on all clusters
		zkzone.ForSortedEsClusters(func(ec *zk.EsCluster) {
			ui.Info(ec.Name)
			ui.Output(callCatRequest(ec.FirstBootstrapNode(), cmd))
		})

		return
	}

	ec := zkzone.NewEsCluster(cluster)
	ui.Output(callCatRequest(ec.FirstBootstrapNode(), cmd))
}

func callCatRequest(endpoint string, api string) string {
	uri := fmt.Sprintf("http://%s/_cat/%s?v", endpoint, api)
	r := gorequest.New()
	_, body, errs := r.Get(uri).End()
	if len(errs) > 0 {
		panic(errs[0])
	}

	return body
}
