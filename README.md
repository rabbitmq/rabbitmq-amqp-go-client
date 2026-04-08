# RabbitMQ AMQP 1.0 Golang Client

This library is meant to be used with RabbitMQ `4.x`.

## Getting Started

- [Getting Started](docs/examples/getting_started)
- [Examples](docs/examples)
  Inside the `docs/examples` directory you will find several examples to get you started.</br>
  Also advanced examples like how to use streams, how to handle reconnections, and how to use TLS.
- Getting started Video tutorial: </br>
[![Getting Started](https://img.youtube.com/vi/iR1JUFh3udI/0.jpg)](https://youtu.be/iR1JUFh3udI)

## Performance test

You can find a performance test in `docs/perf_test/`. 
This client supports two ways to publish messages:
- With the `Publish` method, which is a simple way to publish messages.
- With the `PublishAsync` method, which allows you to publish messages asynchronously 
  and get a confirmation when the message is published. This is useful for high throughput scenarios.
  `PublishAsync` can be tuned with the `PublisherOptions` to achieve better performance.
  Note: With a large MaxInFlight the library can use memory since it tracks all the messages in flight.

## Documentation

- [Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries)



# Packages

The rabbitmq amqp client is a wrapper around the azure amqp client.</b>
You need the following packages to use the rabbitmq amqp client:

- `rabbitmqamqp` - The main package for the rabbitmq amqp client.
- `amqp` - The azure amqp client (You may not need to use this package directly).


## Build from source

- Start the broker with `./.ci/ubuntu/gha-setup.sh start`. Note that this has been tested on Ubuntu 22 with docker.
- `make test` to run the tests
- Stop RabbitMQ with `./.ci/ubuntu/gha-setup.sh stop`


### RabbitMQ Tanzu

Features like AMQP 1.0 over WebSocket, JMS support, and more are only available in Tanzu RabbitMQ 4.0 or later. You can get it from [here](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-oci/4-2/tanzu-rabbitmq-oci-image/site-overview.html).

