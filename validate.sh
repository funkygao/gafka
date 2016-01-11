#!/bin/bash

set -e

# check there are no formatting issues
GOFMT_LINES=`gofmt -l . | grep -v bindata.go | wc -l | xargs`
test $GOFMT_LINES -eq 0 || echo "gofmt needs to be run, ${GOFMT_LINES} files have issues"

GOVER=$(go version | cut -d' ' -f3 | cut -d'.' -f2)
GIT_ID=$(git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(test -n "`git status --porcelain`" && echo "+CHANGES" || true)

if [ $GOVER -gt 4 ]; then
    go test $(go list ./... | grep -v '/bench' | grep -v '/misc' | grep -v '/client') -ldflags "-X github.com/funkygao/gafka.BuildId=${GIT_ID}${GIT_DIRTY} -w"
else
    go test $(go list ./... | grep -v '/bench' | grep -v '/misc' | grep -v '/client') -ldflags "-X github.com/funkygao/gafka.BuildId ${GIT_ID}${GIT_DIRTY} -w"
fi

