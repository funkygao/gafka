package zk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/funkygao/gafka/cmd/kguard/sos"
	log "github.com/funkygao/log4go"
)

// CallSOS will send SOS message to the zone wide kguard leader.
func (this *ZkZone) CallSOS(msg []byte) error {
	log.Critical("calling SOS: %s", string(msg))

	kguards, err := this.KguardInfos()
	if err != nil {
		return err
	}

	leader := kguards[0]
	url := fmt.Sprintf("http://%s:%d", leader.Host, sos.SOSPort) // TODO add User-Agent
	res, err := http.Post(url, "", bytes.NewBuffer(msg))
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusAccepted {
		return fmt.Errorf("SOS fail[%s]: %s", http.StatusText(res.StatusCode), string(body))
	}

	return nil
}
