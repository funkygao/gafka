package zk

import (
	"fmt"
	"net/http"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gorequest"
	log "github.com/funkygao/log4go"
)

// CallSOS will send SOS message to the zone wide kguard leader.
func (this *ZkZone) CallSOS(caller string, msg string) {
	log.Critical("SOS[%s] %s: sending...", caller, msg)

	// kguard leader might float, so refresh on each SOS message
	kguards, err := this.KguardInfos()
	if err != nil {
		log.Error("SOS[%s] %s: %v", caller, msg, err)
		return
	}

	leader := kguards[0]
	request := gorequest.New().Timeout(time.Second * 10)
	res, body, errs := request.Post(fmt.Sprintf("http://%s:%d", leader.Host, telemetry.SOSPort)).
		Set("User-Agent", fmt.Sprintf("sos-go-%s", gafka.BuildId)).
		Set(telemetry.SOSIdentHeader, caller).
		End()
	if len(errs) > 0 {
		log.Error("SOS[%s] %s: %+v", caller, msg, errs)
		return
	}

	if res.StatusCode != http.StatusAccepted {
		log.Error("SOS[%s] %s: HTTP %s %s", caller, msg, http.StatusText(res.StatusCode), body)
		return

	}

	log.Info("SOS[%s] %s: sent ok", caller, msg)
}
