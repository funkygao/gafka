all:build

build:
	go generate ./...
	go install

validate:
	@echo =============Pub=============
	curl -i http://localhost:10891/alive
	@echo 
	@echo =============Sub=============
	curl -i http://localhost:10892/alive
	@echo 
	@echo =============Man=============
	curl -i http://localhost:10893/alive
	@echo
	@echo =============dashboard=============
	curl -i http://localhost:10890/stats

