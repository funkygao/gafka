package main

import (
	"flag"
	"net/http"
	"strings"
	"text/template"

	log "github.com/funkygao/log4go"
)

var host = flag.String("host", "0.0.0.0", "Host")
var port = flag.String("port", "8080", "Port")
var staticContent = flag.String("staticPath", "./swagger-ui", "Path to folder with Swagger UI")
var apiurl = flag.String("api", "http://127.0.0.1", "The base path URI of the API service")

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	log.Trace("%s %s", r.Method, r.RequestURI)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	isJsonRequest := false
	if acceptHeaders, ok := r.Header["Accept"]; ok {
		for _, acceptHeader := range acceptHeaders {
			if strings.Contains(acceptHeader, "json") {
				isJsonRequest = true
				break
			}
		}
	}

	if isJsonRequest {
		w.Write([]byte(resourceListingJson))
	} else {
		http.Redirect(w, r, "/swagger-ui/", http.StatusFound)
	}
}

func ApiDescriptionHandler(w http.ResponseWriter, r *http.Request) {
	log.Trace("%s %s", r.Method, r.RequestURI)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	apiKey := strings.Trim(r.RequestURI, "/")
	if json, ok := apiDescriptionsJson[apiKey]; ok {
		t, e := template.New("desc").Parse(json)
		if e != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		t.Execute(w, *apiurl)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// warning:
// to make swagger server run, you need copy github.com/yvasiyarov/swagger/swagger-ui to current dir.
func runSwaggerServer() {
	flag.Parse()

	http.HandleFunc("/", IndexHandler)
	http.Handle("/swagger-ui/", http.StripPrefix("/swagger-ui/", http.FileServer(http.Dir(*staticContent))))

	for apiKey := range apiDescriptionsJson {
		http.HandleFunc("/"+apiKey+"/", ApiDescriptionHandler)
	}

	serverAddr := *host + ":" + *port
	log.Info("server ready on %s", serverAddr)

	http.ListenAndServe(serverAddr, nil)
}
