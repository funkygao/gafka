package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/stress"
)

var (
	mode  string
	loops int
)

func main() {
	flag.IntVar(&loops, "loops", 1000, "loops in each thread")
	flag.StringVar(&mode, "mode", "gw", "<gw|kafka|http>")
	flag.Parse()

	switch mode {
	case "gw":
		http.DefaultClient.Timeout = time.Second * 30
		stress.RunStress(pubGatewayLoop)

	case "kafka":
		stress.RunStress(pubKafkaLoop)

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
	msg := "hello world"
	for i := 0; i < loops; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: "foobar",
			Value: sarama.StringEncoder(msg),
		})
		if err == nil {
			stress.IncCounter("ok", 1)
		} else {
			stress.IncCounter("fail", 1)
		}
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
	for n := 0; n < loops; n++ {
		req, err := http.NewRequest("POST",
			"http://localhost:9191/topics/v1/foobar?ack=2&timeout=1&retry=3",
			bytes.NewBuffer([]byte("m=hello world")))
		if err != nil {
			log.Fatalf("Error Occured. %+v", err)
			stress.IncCounter("fail", 1)
			return
		}
		req.Header.Set("Pubkey", "mypubkey")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		// use httpClient to send request
		response, err := httpClient.Do(req)
		if err != nil && response == nil {
			stress.IncCounter("fail", 1)
			log.Printf("Error sending request to API endpoint. %+v", err)
		} else {

			if response.StatusCode != http.StatusOK {
				stress.IncCounter("fail", 1)
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
