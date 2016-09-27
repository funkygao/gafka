all:test

GIT_ID=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)

gen:
	REST_DOC=1 go generate

doc:gen

test:
	go test -v -cover -coverprofile=cover -ldflags "-X github.com/funkygao/gafka.BuildId=$(GIT_ID)$(GIT_DIRTY)"

bench:benchmark-cpu-dumb

bench-nometrics:
	go test -v -run=XXX -benchmem -bench=^BenchmarkKatewayPubDummy1KWithoutMetrics -benchtime=20s -cpuprofile=prof.cpu; go tool pprof gateway.test prof.cpu

benchmark-metrics-pubok:
	go test -v -run=XXX -benchmem -bench=^BenchmarkMetricsPubOkCounter -benchtime=20s -cpuprofile=prof.cpu; go tool pprof gateway.test prof.cpu

benchmark-metrics:
	go test -v -run=XXX -benchmem -bench=^BenchmarkMetrics 

benchmark-logappend:
	go test -v -run=none -benchmem -bench=^BenchmarkLogAppend -benchtime=20s 

benchmark-cpu-fastdumb:
	go test -v -run=XXX -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDummy1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof gateway.test prof.cpu

benchmark-cpu-dumb:
	go test -v -run=XXX -benchmem -bench=^BenchmarkKatewayPubDummy1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof gateway.test prof.cpu

benchmark-cpu-pub-kafka:
	go test -v -run=XXX -benchmem -bench=^BenchmarkKatewayPubKafka1K -benchtime=20s -cpuprofile=prof.cpu; go tool pprof gateway.test prof.cpu

benchmark-mem-fastdumb:
	go test -v -run=XXX -benchmem -tags fasthttp -bench=^BenchmarkKatewayPubDummy1K -benchtime=20s -memprofile=prof.mem; go tool pprof gateway.test prof.mem

benchmark-mem-dumb:
	go test -v -run=XXX -benchmem -bench=^BenchmarkKatewayPubDummy1K -benchtime=20s -memprofile=prof.mem
	go tool pprof --lines --inuse_space gateway.test prof.mem
	go tool pprof --lines --alloc_space gateway.test prof.mem

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
	-rm -f gateway.test prof.cpu prof.mem
