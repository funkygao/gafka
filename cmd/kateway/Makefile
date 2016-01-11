all:kafka

GIT_ID=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)

deps:
	@go list ./...

check:
	-go vet
	-golint
	go tool nm -size -sort=size kateway | head -n 50

fast:
	go build -tags fasthttp -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY)"

build:
	go build -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY)"

kafka:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -pubhttps :9291 -subhttp :9192 -manhttps :9293 -store kafka -id 1 -level trace -debughttp ":9194" -keyfile ssl/server.key -certfile ssl/server.pem -debug

bench:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -pubhttps :9291 -subhttp :9192 -level trace -store kafka -id 1 -keyfile ssl/server.key -certfile ssl/server.pem

benchmark-metrics-pubok:
	go test -v -benchmem -bench=^BenchmarkMetricsPubOkCounter -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-metrics:
	go test -v -benchmem -bench=^BenchmarkMetrics 
	go test -v -benchmem -bench=^BenchmarkExtractFromMetricsName

benchmark-logappend:
	go test -v -benchmem -bench=^BenchmarkLogAppend -benchtime=20s 

benchmark-cpu-fastdumb:
	go test -v -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-cpu-dumb:
	go test -v -benchmem -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-mem-fastdumb:
	go test -v -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

benchmark-mem-dumb:
	go test -v -benchmem -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

race:
	@go build -race -gcflags '-m=1'
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

dryrun:dumb

dumb:fast
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -pubhttp :9191 -subhttp :9192 -level debug -store dumb -id 1 -metricsoff=false -breakeron=true -debughttp ":9194" -debug=true

consul:build 
	consul agent -data-dir /tmp/consul &
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -consul localhost:8500 -pubhttp :9191 -subhttp :9192 -level debug -debug -store dumb -id 1 

pprof-cpu:
	go tool pprof http://localhost:9194/debug/pprof/profile?seconds=30

pprof-mem:
	go tool pprof http://localhost:9194/debug/pprof/heap

pprof-block:
	go tool pprof http://localhost:9194/debug/pprof/blocking

genkey:
	@mkdir ssl
	openssl genrsa -out ssl/server.key 2048
	openssl req -new -x509 -key ssl/server.key -out ssl/server.pem -days 3650
	@echo check out ssl/

clean:
	-rm -f kateway.test kateway prof.cpu prof.mem
