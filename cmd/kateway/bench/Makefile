bench:sync

sync:bench.go
	go run bench.go -neat

async:bench.go
	go run bench.go -async -neat

kafka:bench.go
	go run bench.go -neat -mode kafka

ak:bench.go
	go run bench.go -neat -mode kafka -async

http:httpd.go 
	go build -o httpdserver httpd.go
	./httpdserver -mode standalone&
	go run bench.go -neat -mode http

hp:httpd.go 
	go build -o httpdserver httpd.go
	./httpdserver -p 9080&
	./httpdserver -mode proxy&
	go run bench.go -neat -mode http

clean:
	rm -f httpdserver
	killall httpdserver
