# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN = $(shell go env GOPATH)/bin
else
GOBIN = $(shell go env GOBIN)
endif

all: test

format:
	go fmt ./...

vet:
	go vet ./pkg/rabbitmqamqp
	go vet ./docs/examples/...

STATICCHECK ?= $(GOBIN)/staticcheck
STATICCHECK_VERSION ?= latest
$(STATICCHECK):
	go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
check: $(STATICCHECK)
	$(STATICCHECK) ./pkg/rabbitmqamqp
	$(STATICCHECK) ./docs/examples/...





test: format vet check
	cd ./pkg/rabbitmqamqp && go run -mod=mod github.com/onsi/ginkgo/v2/ginkgo  \
                --randomize-all --randomize-suites \
                --cover --coverprofile=coverage.txt --covermode=atomic \
                --race



rabbitmq-server-start:
	 ./.ci/ubuntu/gha-setup.sh start pull

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop
