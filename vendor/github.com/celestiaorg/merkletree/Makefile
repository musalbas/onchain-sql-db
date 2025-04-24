all: install

REBUILD:
	@touch debug*.go

dependencies:
	go get -u github.com/dvyukov/go-fuzz/go-fuzz
	go get -u github.com/dvyukov/go-fuzz/go-fuzz-build
	go get -u gitlab.com/NebulousLabs/fastrand
	go get -u gitlab.com/NebulousLabs/errors
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install

install: REBUILD
	go install

lint:
	gometalinter --disable-all --enable=errcheck --enable=vet --enable=gofmt ./...

test: REBUILD
	go test -v -tags='debug' -timeout=600s

test-short: REBUILD
	go test -short -v -tags='debug' -timeout=6s

cover: REBUILD
	go test -coverprofile=coverage.out -v -race -tags='debug' ./...

fuzz: REBUILD
	go install -tags='debug gofuzz'
	go-fuzz-build gitlab.com/NebulousLabs/merkletree
	go-fuzz -bin=./merkletree-fuzz.zip -workdir=fuzz

.PHONY: all REBUILD dependencies install test test-short cover fuzz benchmark
