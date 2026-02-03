.PHONY: test test-unit test-integration

test: test-unit

test-unit:
	go test ./... -count=1

test-integration:
	go test -tags=integration ./... -count=1
