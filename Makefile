all: format test

format:

test:
	cd rabbitmq_amqp && go run -mod=mod github.com/onsi/ginkgo/v2/ginkgo  \
                --randomize-all --randomize-suites \
                --cover --coverprofile=coverage.txt --covermode=atomic \
                --race



rabbitmq-server-start-arm:
	 ./.ci/ubuntu/gha-setup.sh start pull arm 

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop