bench:sync

pub:pub.go
	@echo keep pub to kateway to check mem/thread leakeage
	@go run pub.go

sync:bench.go
	@echo bench against kateway in sync pub mode
	@go run bench.go -neat -mode gw -addr "localhost:9191"

async:bench.go
	@echo bench against kateway in async pub mode
	@go run bench.go -async -neat -mode gw -addr "localhost:9191"

kafka:bench.go
	@echo bench against kafka directly in sync pub mode
	@go run bench.go -neat -mode kafka

ak:bench.go
	@echo bench against kafka directly in async pub mode
	@go run bench.go -neat -mode kafka -async

fasthttp:fasthttpd.go 
	go build -o fasthttpdserver fasthttpd.go
	GOGC=800 GODEBUG=gctrace=1 ./fasthttpdserver &
	go run bench.go -neat -mode http

http:httpd.go 
	go build -o httpdserver httpd.go
	GOGC=800 GODEBUG=gctrace=1 ./httpdserver -mode standalone&
	go run bench.go -neat -mode http

sub:sub.go
	go run sub.go -n 200000

goproxy:httpd.go 
	go build -o httpdserver httpd.go
	./httpdserver -p 9080&
	./httpdserver -mode goproxy&
	go run bench.go -neat -mode http

proxy:httpd.go 
	go build -o httpdserver httpd.go
	./httpdserver -p 9080&
	./httpdserver -mode proxy&
	go run bench.go -neat -mode http

clean:
	-rm -f httpdserver fasthttpdserver
	-killall httpdserver fasthttpdserver
