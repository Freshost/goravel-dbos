.PHONY: build format format-check test test-postgres race vet ci

build:
	go build ./...

format:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

format-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))" || \
		(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'); exit 1)

test:
	go test -count=1 ./...

test-postgres:
	@test -n "$$GORAVEL_DBOS_TEST_DATABASE_URL" || \
		(echo 'GORAVEL_DBOS_TEST_DATABASE_URL is required'; exit 1)
	go test -count=1 -run '^TestPostgres' .

race:
	go test -race -count=1 ./...

vet:
	go vet ./...

ci: format-check build vet test race
