all: test

format:
	go fmt ./...

vet:
	go vet ./pkg/rabbitmq_amqp

test: format vet
	cd ./pkg/rabbitmq_amqp && go run -mod=mod github.com/onsi/ginkgo/v2/ginkgo  \
                --randomize-all --randomize-suites \
                --cover --coverprofile=coverage.txt --covermode=atomic \
                --race



rabbitmq-server-start-arm:
	 ./.ci/ubuntu/gha-setup.sh start pull arm 

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop
