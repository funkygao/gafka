package main

import (
	"net"
	"net/http"

	"github.com/funkygao/golib/stress"
)

func init() {
	http.DefaultClient.Timeout = time.Second * 30
}

func main() {
	stress.RunStress(pub)
}

func pub(seq int) {
	to := 3
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   to * time.Second,
				KeepAlive: 60 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: to * time.Second,
		},
	}

	req, err := http.NewRequest("POST", endPoint, bytes.NewBuffer([]byte("Post this data")))
	if err != nil {
		log.Fatalf("Error Occured. %+v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// use httpClient to send request
	response, err := httpClient.Do(req)
	if err != nil && response == nil {
		log.Fatalf("Error sending request to API endpoint. %+v", err)
	} else {
		// Close the connection to reuse it
		defer response.Body.Close()

		// Let's check if the work actually is done
		// We have seen inconsistencies even when we get 200 OK response
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf("Couldn't parse response body. %+v", err)
		}

		log.Println("Response Body:", string(body))
	}

}
