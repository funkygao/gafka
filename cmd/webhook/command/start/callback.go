package start

import (
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
)

func (this *Start) Callback(endpoint string, msg *sarama.ConsumerMessage) error {
	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	buf.Reset()
	buf.Write(msg.Value)

	req, err := http.NewRequest("POST", endpoint, buf)
	if err != nil {
		return err
	}

	// setup http headers
	req.Header.Set("User-Agent", UserAgent)

	// invoke the callback over the wire
	response, err := this.callbackConn.Do(req)
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	// reuse the connection
	response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		return errors.New(string(b))
	}

	if this.debug {
		log.Debug("%s -> %s", endpoint, string(msg.Value))
	}

	return nil
}
