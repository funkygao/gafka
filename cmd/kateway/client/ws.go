package main

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host     = "10.1.114.159:9192"
	myAppid  = "appX"
	mySubkey = "mysubkey"
	hisAppid = "app1"
	topic    = "foo"
	group    = "groupXXX"
	ver      = "v1"
	sleep    = time.Second * 2
)

func main() {
	u := url.URL{
		Scheme: "ws",
		Host:   host,
		Path:   fmt.Sprintf("/ws/topics/%s/%s/%s/%s", hisAppid, topic, ver, group),
	}
	log.Printf("connecting to %+v", u)

	conn, httpResponse, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer conn.Close()

	_ = httpResponse

	log.Printf("connected to: %s", host)

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if err == io.EOF {
				log.Printf("disconnected with %s", u.String())
				return
			}

			log.Printf("read: %v", err)
			return
		}

		log.Printf("-> msgtype:%d msg:%s", messageType, string(message))
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}
