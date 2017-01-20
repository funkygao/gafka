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

	"github.com/EverythingMe/go-disque/disque"
	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/stress"
	"github.com/garyburd/redigo/redis"
)

var (
	addr          string
	mode          string
	topic         string
	loops         int
	suppressError bool
	pubkey        string
	appid         string
	sleep         time.Duration
	ver           string
	sz            int
	async         bool
)

func main() {
	flag.StringVar(&addr, "addr", "http://localhost:9191/", "kateway pub addr, must start with http or https")
	flag.IntVar(&loops, "loops", 1000, "loops in each thread")
	flag.IntVar(&sz, "size", 200, "each pub message size")
	flag.StringVar(&topic, "topic", "foobar", "pub topic")
	flag.StringVar(&mode, "mode", "gw", "<gw|kafka|http|redis>")
	flag.StringVar(&appid, "appid", "app1", "appid of pub")
	flag.StringVar(&pubkey, "pubkey", "mypubkey", "pubkey")
	flag.StringVar(&ver, "ver", "v1", "pub topic version")
	flag.BoolVar(&async, "async", false, "async pub")
	flag.DurationVar(&sleep, "sleep", 0, "sleep between pub")
	flag.BoolVar(&suppressError, "noerr", false, "suppress error output")
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

	case "redis":
		stress.RunStress(redisLoop)

	case "disque":
		stress.RunStress(disqueLoop)

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

func dial(addr string) (redis.Conn, error) {
	return redis.Dial("tcp", addr)
}

func disqueLoop(seq int) {
	pool := disque.NewPool(disque.DialFunc(dial), "localhost:7711")
	c, err := pool.Get()
	if err != nil {
		stress.IncCounter("fail", 1)
		log.Println(err)
		return
	}

	defer c.Close()
	for i := 0; i < loops; i++ {
		_, err = c.Add(disque.AddRequest{
			Job: disque.Job{
				Queue: "demo",
				Data:  []byte(fmt.Sprintf("hello world from %d:%d", seq, i)),
			},
			Delay:   time.Minute,
			Timeout: time.Millisecond * 100,
		})

		if err == nil {
			stress.IncCounter("ok", 1)
		} else {
			if !suppressError {
				log.Println(err)
			}
			stress.IncCounter("fail", 1)
		}
	}
}

func redisLoop(seq int) {
	conn, err := redis.DialTimeout("tcp", ":6379", 0, 1*time.Second, 1*time.Second)
	if err != nil {
		stress.IncCounter("fail", 1)
		log.Println(err)
		return
	}

	defer conn.Close()
	msg := strings.Repeat("X", sz)
	for i := 0; i < loops; i++ {
		_, err := conn.Do("SET", "key", msg)
		if err == nil {
			stress.IncCounter("ok", 1)
		} else {
			if !suppressError {
				log.Println(err)
			}
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
	url := fmt.Sprintf("%s/topics/%s/%s?", addr, topic, ver)
	if async {
		url += "async=1"
	}
	for n := 0; n < loops; n++ {
		req, err := http.NewRequest("POST", url,
			bytes.NewBuffer([]byte(strings.Repeat("X", sz))))
		if err != nil {
			log.Fatalf("Error Occurred: %+v", err)
			stress.IncCounter("fail", 1)
			return
		}

		req.Header.Set("Appid", appid)
		req.Header.Set("Pubkey", pubkey)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		// use httpClient to send request
		response, err := httpClient.Do(req)
		if err != nil && response == nil {
			stress.IncCounter("fail", 1)
			if !suppressError {
				log.Printf("Error sending request to API endpoint. %+v", err)
			}
		} else {
			if response.StatusCode != http.StatusCreated {
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

		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}
