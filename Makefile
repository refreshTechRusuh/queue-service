run:
	go run ./cmd
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/api ./cmd
test:
	go test -v ./...
test-race:
	go test -race -count=1 ./...
unit-test-coverage:
	go test -v -covermode=count $$(go list ./... | grep -v '/test/') -coverprofile=coverage.cov
	go tool cover -func=coverage.cov
mod-tidy:
	go mod tidy
golint:
	golangci-lint run --timeout 5m
docker-build: build
	docker build -f Dockerfile -t queue-service:latest .
