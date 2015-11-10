package command

import (
	"github.com/funkygao/gafka/config"
)

var (
	cf *config.Config
)

func init() {
	cf = config.LoadConfig("/etc/gafka.cf")
}
