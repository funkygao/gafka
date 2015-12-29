package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/stress"
)

var (
	addr          string
	mode          string
	topic         string
	loops         int
	suppressError bool
	appid         string
	sz            int
	async         bool
)

func main() {
	flag.StringVar(&addr, "addr", "http://10.213.1.210:9191/", "pub addr")
	flag.IntVar(&loops, "loops", 1000, "loops in each thread")
	flag.IntVar(&sz, "size", 200, "each pub message size")
	flag.StringVar(&topic, "topic", "foobar", "pub topic")
	flag.StringVar(&mode, "mode", "gw", "<gw|kafka|http>")
	flag.StringVar(&appid, "appid", "app1", "appid of pub")
	flag.BoolVar(&async, "async", false, "async pub")
	flag.BoolVar(&suppressError, "noerr", true, "suppress error output")
	flag.Parse()

	switch mode {
	case "gw":
		http.DefaultClient.Timeout = time.Second * 30
		stress.RunStress(pubGatewayLoop)

	case "kafka":
		if async {
			stress.RunStress(pubKafkaAsyncLoop)
		} else {
			stress.RunStress(pubKafkaLoop)
		}

	case "http":
		http.DefaultClient.Timeout = time.Second * 30
		stress.RunStress(getHttpLoop)
	}

}

func getHttpLoop(seq int) {
	client := createHttpClient()
	req, _ := http.NewRequest("GET", "http://localhost:9090/", nil)
	for i := 0; i < loops; i++ {
		response, err := client.Do(req)
		if err == nil {
			ioutil.ReadAll(response.Body)
			response.Body.Close() // reuse the connection

			stress.IncCounter("ok", 1)
		} else {
			stress.IncCounter("fail", 1)
			//log.Println(err)
		}
	}
}

func pubKafkaLoop(seq int) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, cf)
	if err != nil {
		stress.IncCounter("fail", 1)
		log.Println(err)
		return
	}

	defer producer.Close()
	msg := strings.Repeat("X", sz)
	for i := 0; i < loops; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		})
		if err == nil {
			stress.IncCounter("ok", 1)
		} else {
			stress.IncCounter("fail", 1)
		}
	}

}

func pubKafkaAsyncLoop(seq int) {
	cf := sarama.NewConfig()
	cf.Producer.Flush.Frequency = time.Second * 10
	cf.Producer.Flush.Messages = 1000
	cf.Producer.Flush.MaxMessages = 1000
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, cf)
	if err != nil {
		stress.IncCounter("fail", 1)
		log.Println(err)
		return
	}

	defer producer.Close()
	msg := strings.Repeat("X", sz)
	for i := 0; i < loops; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		stress.IncCounter("ok", 1)
	}

}

func createHttpClient() *http.Client {
	timeout := 3 * time.Second
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 60 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: timeout,
		},
	}

	return httpClient
}

func pubGatewayLoop(seq int) {
	httpClient := createHttpClient()
	url := fmt.Sprintf("http://%s/topics/%s/v1?", addr, topic)
	if async {
		url += "async=1"
	}
	for n := 0; n < loops; n++ {
		req, err := http.NewRequest("POST", url,
			bytes.NewBuffer([]byte(strings.Repeat("X", sz))))
		if err != nil {
			log.Fatalf("Error Occured. %+v", err)
			stress.IncCounter("fail", 1)
			return
		}

		req.Header.Set("Appid", appid)
		req.Header.Set("Pubkey", "mypubkey")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		// use httpClient to send request
		response, err := httpClient.Do(req)
		if err != nil && response == nil {
			stress.IncCounter("fail", 1)
			if !suppressError {
				log.Printf("Error sending request to API endpoint. %+v", err)
			}
		} else {
			if response.StatusCode != http.StatusOK {
				stress.IncCounter("fail", 1)
				if !suppressError {
					log.Printf("Error sending request to API endpoint. %+v", response.Status)
				}
				return
			}

			// Let's check if the work actually is done
			// We have seen inconsistencies even when we get 200 OK response
			body, err := ioutil.ReadAll(response.Body)
			if err != nil {
				log.Fatalf("Couldn't parse response body. %+v", err)
			}

			// Close the connection to reuse it
			response.Body.Close()

			stress.IncCounter("ok", 1)

			if false {
				log.Println("Response Body:", string(body))
			}
		}
	}
}
