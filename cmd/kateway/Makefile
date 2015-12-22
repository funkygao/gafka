all:kafka

deps:
	@go list ./...

fast:
	go build -tags fasthttp

build:
	go build

bench:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -pubhttp :9191 -subhttp :9192 -level info -store kafka -id 1 

benchmark-metrics-pubok:
	go test -v -benchmem -bench=^BenchmarkMetricsPubOkCounter -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-metrics:
	go test -v -benchmem -bench=^BenchmarkMetrics 
	go test -v -benchmem -bench=^BenchmarkExtractFromMetricsName

benchmark-cpu-fastdumb:
	go test -v -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-cpu-dumb:
	go test -v -benchmem -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-mem-fastdumb:
	go test -v -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

benchmark-mem-dumb:
	go test -v -benchmem -bench=^BenchmarkKatewayPubDumb1K -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

kafka:fast
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -pubhttp :9191 -subhttp :9192 -store kafka -id 1 -level info -cpuprof

race:
	@go build -race -gcflags '-m=1'
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

dryrun:dumb

dumb:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -pubhttp :9191 -subhttp :9192 -level info -store dumb -id 1 -cpuprof -metricsoff=true -breakeron=false

consul:build 
	consul agent -data-dir /tmp/consul &
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -consul localhost:8500 -pubhttp :9191 -subhttp :9192 -level debug -debug -store dumb -id 1 

prof:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local  -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 -memprof -cpuprof -blockprof
	go tool pprof kateway prof/cpu.pprof

clean:
	-rm -f kateway.test kateway
