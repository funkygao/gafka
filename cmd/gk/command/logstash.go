package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Logstash struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Logstash) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("logstash", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.showSampleConfig()

	return
}

func (this *Logstash) showSampleConfig() {
	config := `
input {
    file {
        path => "/var/wd/ehaproxy/ehaproxy.log"
        type => "ehaproxy"
    }
    file {
        path => "/var/wd/ehaproxy/panic"
        type => "ehaproxy_panic"
    }
    file {
        path => "/var/wd/ehaproxy/logs/haproxy.log"
        type => "haproxy"
    }
    file {
        path => "/var/wd/kateway/kateway.log"
        type => "kateway"
    }
    file {
        path => "/var/wd/kateway/panic"
        type => "kateway_panic"
    }
}

filter {
    multiline {
        pattern => "^201" # e,g. this line begins with 2017-01-22
        what => "previous"
        negate => true
    }
}

output {
    if [type] == "pubsub" {
         http {
             http_method => "post"
             url => "http://pub.test.mycorp.com:10191/v1/raw/msgs/cluster/topic"
             workers => 8 # each worker has 25 http connection, totals: 200
             headers => {
                "User-Agent" => "logstash"
             }
         }
    } else {
        kafka {
            bootstrap_servers => "k11003a.mycorp.kfk.com:11003,k11003b.mycorp.kfk.com:11003"
            topic_id => "pubsub_log"
            metadata_max_age_ms => 300000
            workers => 1
            retries => 1
        }
    }
}`
	this.Ui.Output(strings.TrimSpace(config))
}

func (*Logstash) Synopsis() string {
	return "Sample configuration for logstash"
}

func (this *Logstash) Help() string {
	help := fmt.Sprintf(`
Usage: %s logstash [options]

    %s    

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
