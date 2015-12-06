test:
	@#go build -race -gcflags '-m=1'
	@go build
	GOGC=800 GODEBUG=gctrace=1 ./kateway -zone local -cluster me -pubhttp :9191 -subhttp :9192 -level debug -debug -store kafka -id 1 

