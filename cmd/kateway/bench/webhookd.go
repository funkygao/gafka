// webhookd is a webhook endpoint that is used to demo kateway webhook feature.
package main

import (
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", handle)
	log.Fatal(http.ListenAndServe(":9199", nil))
}

func handle(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("%s %s[%s] %+v %s", r.Method, r.RequestURI, r.UserAgent(), r.Header, string(body))
}
