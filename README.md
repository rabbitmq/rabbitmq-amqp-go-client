# RabbitMQ AMQP 1.0 .Golang Client

This library is meant to be used with RabbitMQ 4.0. 
Suitable for testing in pre-production environments.


## Getting Started

You can find an example in: `docs/examples/getting_started`

## Examples

You can find more examples in: `docs/examples`

## Build from source

- Start the broker with `./.ci/ubuntu/gha-setup.sh start`. Note that this has been tested on Ubuntu 22 with docker.
- `make test` to run the tests
- Stop RabbitMQ with `./.ci/ubuntu/gha-setup.sh stop`


