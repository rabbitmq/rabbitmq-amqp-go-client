# RabbitMQ AMQP 1.0 Golang Client

This library is meant to be used with RabbitMQ 4.0. 
Suitable for testing in pre-production environments.


## Getting Started

- [Getting Started](docs/examples/getting_started)
- [Examples](docs/examples)
- Getting started Video tutorial:
[![Getting Started](https://img.youtube.com/vi/iR1JUFh3udI/0.jpg)](https://youtu.be/iR1JUFh3udI)



## Documentation

- [Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries) (work in progress for this client)



# Packages

The rabbitmq amqp client is a wrapper around the azure amqp client.</b>
You need the following packages to use the rabbitmq amqp client:

- `rabbitmqamqp` - The main package for the rabbitmq amqp client.
- `amqp` - The azure amqp client (You may not need to use this package directly).


## Build from source

- Start the broker with `./.ci/ubuntu/gha-setup.sh start`. Note that this has been tested on Ubuntu 22 with docker.
- `make test` to run the tests
- Stop RabbitMQ with `./.ci/ubuntu/gha-setup.sh stop`


