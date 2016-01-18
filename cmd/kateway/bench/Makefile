bench:kwsync

pub:pub.go
	@echo keep pub to kateway to check mem/thread leakeage
	@go run pub.go -t foo -sleep 3s -c 1 -sz 50

sub:sub.go
	@echo sub from kateway
	go run sub.go -n 10 -t foo -sleep 2s

kwsync:bench.go
	@echo bench against kateway in sync pub mode
	@go run bench.go -neat -mode gw -addr "http://localhost:9191"

kwsyncs:bench.go
	@echo bench against kateway in sync pub mode
	@go run bench.go -neat -mode gw -addr "https://localhost:9191"

kwasync:bench.go
	@echo bench against kateway in async pub mode
	@go run bench.go -async -neat -mode gw -addr "http://localhost:9191"

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

redis:bench.go
	@echo bench against redis set/get
	@go run bench.go -neat -mode redis -noerr=true

http:httpd.go 
	go build -o httpdserver httpd.go
	GOGC=800 GODEBUG=gctrace=1 ./httpdserver -mode standalone&
	go run bench.go -neat -mode http

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
