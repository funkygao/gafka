all:test

GIT_ID=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)

check:
	-go vet
	-golint
	go tool nm -size -sort=size kateway | head -n 50

test:
	go test -v -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY)"

benchmark-metrics-pubok:
	go test -v -benchmem -bench=^BenchmarkMetricsPubOkCounter -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-metrics:
	go test -v -benchmem -bench=^BenchmarkMetrics 

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

pprof-cpu:
	go tool pprof http://localhost:9194/debug/pprof/profile?seconds=30

pprof-mem:
	go tool pprof http://localhost:9194/debug/pprof/heap

pprof-block:
	go tool pprof http://localhost:9194/debug/pprof/blocking

prof-http-pkg:
	go test -run=none -bench=ClientServerParallel4 -cpuprofile=cpuprof net/http
	go tool pprof http.test cpuprof

clean:
	-rm -f kateway.test kateway prof.cpu prof.mem
