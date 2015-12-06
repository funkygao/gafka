all:kafka

deps:
	@go list ./...

build:
	@go build

kafka:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

race:
	@go build -race -gcflags '-m=1'
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

dumb:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store dumb -id 1 

consul:build 
	consul agent -data-dir /tmp/consul &
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -consul localhost:8500 -pubhttp :9191 -subhttp :9192 -level debug -debug -store dumb -id 1 

prof:build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 -memprof -cpuprof -blockprof
	go tool pprof kateway prof/cpu.pprof
