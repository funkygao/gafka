all:build

build:
	go generate ./...
	go install

validate:
	@echo =============Pub=============
	curl -i http://localhost:9191/alive
	@echo 
	@echo =============Sub=============
	curl -i http://localhost:9192/alive
	@echo 
	@echo =============Man=============
	curl -i http://localhost:9193/alive
	@echo
	@echo =============dashboard=============
	curl -i http://localhost:10894/v1/status
	@echo
	@echo =============diagnostics===========
	curl -i http://localhost:10120/debug/pprof/

