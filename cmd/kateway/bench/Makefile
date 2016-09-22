bench:kwsync

job:job.go
	# job does not support tag
	@go run job.go -step 1 -t foobar -c 1 -sz 50 -sleep 3s 

pubn:pub.go
	@go run pub.go -step 1 -t foobar -c 50 -sz 50 -tag hello -sleep 3s 

puba:pub.go
	@go run pub.go -step 1 -t foobar -c 1 -sz 50 -tag hello -sleep 3s -async

pub:pub.go
	@echo keep pub to kateway to check mem/thread leakeage
	@go run pub.go -step 1 -t foobar -c 1 -sz 50 -tag hello -sleep 3s 

ack:ack.go
	@echo sub from kateway
	go run ack.go -t foobar -sleep 5s

subx:sub.go
	@echo sub from kateway
	go run sub.go -t foobar -mode subx -sleep 5s

s:
	go run pubsub.go -c1 1 -c2 1 -mode sub -appid app2 -key xxx -subappid app1 -ep 10.1.114.159:9192 -topic foobar -group bench_go -batch 2000  -sleep 0s -debug=false

subn:sub.go
	go run sub.go -t foobar -mode sub -sleep 1s -c 150

sub:sub.go
	go run sub.go -t foobar -mode sub -sleep 5s
	#go run sub.go -t foobar -mode sub -sleep 1s -tag hello

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

disque:bench.go
	@echo bench again disque
	@go run bench.go -neat -mode disque

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
