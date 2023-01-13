# To try different version of Go
GO := go

generate:
	go generate ./...

test: generate
	go test -mod=readonly -race -coverpkg=./... -covermode=atomic -coverprofile=coverage.out.tmp ./...
	cat coverage.out.tmp | grep -v "/mock_" > coverage.txt #IGNORE MOCKS
	go tool cover -html=coverage.txt -o coverage.html

lint:
	golangci-lint run ./...
	go fmt ./...