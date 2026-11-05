.PHONY: fmt vet lint test vuln check

fmt:
	gofmt -l -w .

vet:
	go vet ./...

lint:
	golangci-lint run

test:
	go test -race -count=1 ./...

vuln:
	govulncheck ./...

check: vet lint test vuln