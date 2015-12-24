#!/bin/bash -e

if [[ $1 = "-h" ]]; then
    echo "build.sh [-gc|-loc|-install|kw|-race]"
    exit
fi

if [[ $1 = "-loc" ]]; then
    find . -name '*.go' | xargs wc -l | sort -n | tail -1
    exit
fi

PREFIX=$GOPATH
BINDIR=${PREFIX}/bin

VER=0.2.3stable
GOVER=$(go version | cut -d' ' -f3 | cut -d'.' -f2)
# get the git commit
GIT_ID=$(git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=$(test -n "`git status --porcelain`" && echo "+CHANGES" || true)

BUILD_FLAGS=''
if [[ $1 = "-race" ]]; then
    BUILD_FLAGS="$BUILD_FLAGS -race"
fi
if [[ $1 = "-gc" ]]; then
    BUILD_FLAGS="$BUILD_FLAGS -gcflags '-m=1'"
fi

BUILD_PKG=cmd/gk
BUILD_BIN=gk
if [[ $1 = "zk" ]]; then
    BUILD_PKG=cmd/zk
    BUILD_BIN=zk
fi
if [[ $1 = "kw" ]]; then
    BUILD_PKG=cmd/kateway
    BUILD_BIN=kateway
fi

cd $BUILD_PKG
go generate ./...
if [ $GOVER -gt 4 ]; then
    go build $BUILD_FLAGS -tags fasthttp -ldflags "-X github.com/funkygao/gafka.Version=$VER -X github.com/funkygao/gafka.BuildId=${GIT_ID}${GIT_DIRTY} -w"
else
    go build $BUILD_FLAGS -tags fasthttp -ldflags "-X github.com/funkygao/gafka.Version $VER -X github.com/funkygao/gafka.BuildId ${GIT_ID}${GIT_DIRTY} -w"
fi

if [[ $1 = "-install" ]]; then
    install -m 755 gk $PREFIX/bin
fi

#---------
# show ver
#---------
./$BUILD_BIN -version
