package command

import (
	"fmt"
	"strings"

	"github.com/drnic/consul-discovery"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Consul struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Consul) Run(args []string) (exitCode int) {
	cf := consuldiscovery.DefaultConfig()
	cf.Address = ctx.ConsulBootstrap()
	client, _ := consuldiscovery.NewClient(cf)
	services, err := client.CatalogServices()
	swallow(err)

	for _, service := range services {
		serviceNodes, _ := client.CatalogServiceByName(service.Name)
		this.Ui.Output(fmt.Sprintf("%25s: %#v", service.Name, serviceNodes))
	}
	return
}

func (*Consul) Synopsis() string {
	return "Service discovery from consul"
}

func (this *Consul) Help() string {
	help := fmt.Sprintf(`
Usage: %s consul [options]

    Service discovery from consul

`, this.Cmd)
	return strings.TrimSpace(help)
}
