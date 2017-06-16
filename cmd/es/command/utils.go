package command

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
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
			out := callCatRequest(ec.FirstBootstrapNode(), cmd)
			lines := strings.Split(out, "\n")
			if len(lines) > 2 {
				ui.Outputf("%s (%d %s)", color.Green(ec.Name), len(lines)-1, cmd)
			} else {
				ui.Info(ec.Name)
			}
			ui.Output(out)
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
