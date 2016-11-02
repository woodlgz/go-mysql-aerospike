all: build

build: build-aerospike

build-aerospike:
	godep go build -o bin/go-mysql-aerospike ./cmd/go-mysql-aerospike

test:
	godep go test --race ./...

clean:
	godep go clean -i ./...
	@rm -rf bin
