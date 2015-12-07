all:kafka

deps:
	@go list ./...

build:
	@go build

bench:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level info -store kafka -id 1 

benchmark-cpu-kafka:build
	go test -v -benchmem -bench=^BenchmarkPubKafka -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-mem-kafka:build
	go test -v -benchmem -bench=^BenchmarkPubKafka -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

benchmark-cpu-dumb:build
	go test -v -benchmem -bench=^BenchmarkPubDumb -benchtime=20s -cpuprofile=prof.cpu; go tool pprof kateway.test prof.cpu

benchmark-mem-dumb:build
	go test -v -benchmem -bench=^BenchmarkPubDumb -benchtime=20s -memprofile=prof.mem; go tool pprof kateway.test prof.mem

kafka:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

race:
	@go build -race -gcflags '-m=1'
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

dumb:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level info -store dumb -id 1 

consul:build 
	consul agent -data-dir /tmp/consul &
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -consul localhost:8500 -pubhttp :9191 -subhttp :9192 -level debug -debug -store dumb -id 1 

prof:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 -memprof -cpuprof -blockprof
	go tool pprof kateway prof/cpu.pprof

clean:
	rm -f kateway.test kateway
