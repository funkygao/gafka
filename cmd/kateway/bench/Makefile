bench:sync

sync:bench.go
	@echo bench against kateway in sync pub mode
	go run bench.go -neat -mode gw

async:bench.go
	@echo bench against kateway in async pub mode
	go run bench.go -async -neat -mode gw

kafka:bench.go
	@echo bench against kafka directly in sync pub mode
	go run bench.go -neat -mode kafka

ak:bench.go
	@echo bench against kafka directly in async pub mode
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
