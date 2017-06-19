all:kafka

GIT_ID=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)
BUILD_TIME=$(shell date '+%Y-%m-%d-%H:%M:%S')

deps:
	@go list ./...

check:
	-go vet
	-golint
	-go tool vet -shadow .
	#go get github.com/barakmich/go-nyet
	-go-nyet ./...
	go tool nm -size -sort=size kateway | head -n 50

doc:
	@cd gateway; make doc

build:
	go build -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY) -X github.com/funkygao/gafka.BuiltAt=$(BUILD_TIME)"

k1:build
	# to debug haproxy blance algorithm, make k1; make k2
	GOGC=800 GODEBUG=gctrace=0 ./kateway -zone local -pubhttp ":8091" -subhttp ":8092" -manhttp ":8093" -store kafka -id 1 -level debug -mstore dummy -accesslog -influxdbaddr= -httppanic -gzip=false -jstore dummy

k2:build
	GOGC=800 GODEBUG=gctrace=0 ./kateway -zone local -pubhttp ":7091" -subhttp ":7092" -manhttp ":7093" -store kafka -id 2 -level debug -mstore dummy -accesslog -influxdbaddr= -httppanic -gzip=false -jstore dummy

swagger:
	go generate
	go build -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY) -X github.com/funkygao/gafka.BuiltAt=$(BUILD_TIME)"
	./kateway -swagger

flush:buildrace
	GOGC=800 GODEBUG=gctrace=0 ./kateway -zone local -pubhttp :9191 -subhttps :9292 -subhttp :9192 -manhttp :9193 -pubhttps :9291 -manhttps :9293 -store kafka -id 1 -level debug -debughttp ":9194" -keyfile ssl/server.key -certfile ssl/server.pem -mstore dummy -accesslog -influxdbaddr=http://localhost:333 -httppanic -gzip=true -hhflush

kafka:buildrace
	GOGC=800 GODEBUG=gctrace=0 ./kateway -zone local -pubhttp :9191 -subhttps :9292 -subhttp :9192 -manhttp :9193 -pubhttps :9291 -manhttps :9293 -store kafka -id 1 -level debug -debughttp ":9194" -keyfile ssl/server.key -certfile ssl/server.pem -mstore dummy -jstore dummy -accesslog -influxdbaddr=http://localhost:333 -httppanic -gzip=true -standbysub=false -maxreq 2
	#GOGC=800 GODEBUG="gctrace=1,schedtrace=1000,scheddetail=1" ./kateway -zone local -pubhttp :9191 -pubhttps :9291 -subhttp :9192 -manhttps :9293 -store kafka -id 1 -level trace -debughttp ":9194" -keyfile ssl/server.key -certfile ssl/server.pem -debug

dummy:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -subhttp :9192 -level debug -store dummy -id 1 -metricsoff=false -debughttp ":9194" -debug=true -mstore dummy -accesslog 

bench:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -pubhttps :9291 -subhttp :9192 -level trace -store kafka -id 1 -keyfile ssl/server.key -certfile ssl/server.pem -mstore dummy

flame:
	go-torch -u http://localhost:9194/debug/pprof/profile --title=kateway

buildrace:
	@go build -race -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY) -X github.com/funkygao/gafka.BuiltAt=$(BUILD_TIME)"

gdb:
	@go build -gcflags "-N -l" -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY) -X github.com/funkygao/gafka.BuiltAt=$(BUILD_TIME)"

cpu:
	go tool pprof http://localhost:9194/debug/pprof/profile?seconds=30

heap:
	go tool pprof kateway http://localhost:9194/debug/pprof/heap
	go tool pprof --alloc_space kateway http://localhost:9194/debug/pprof/heap

objects:
	go tool pprof --alloc_objects kateway http://localhost:9194/debug/pprof/heap

genkey:
	@mkdir ssl
	openssl genrsa -out ssl/server.key 2048
	openssl req -new -x509 -key ssl/server.key -out ssl/server.pem -days 3650
	@echo check out ssl/

clean:
	-rm -f kateway.test kateway prof.cpu prof.mem *.log*
