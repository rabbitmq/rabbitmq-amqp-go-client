### AMQP 1.0 .Golang Client Examples


- [Getting Started](getting_started) - A simple example to get you started.
- [Quorum single-active-consumer notification](qq_single_active_notification) - Quorum queue SAC: FLOW link-state `rabbitmq:active` via `ConsumerOptions.SingleActiveConsumerStateChanged` (RabbitMQ 4.3+). Mirrors the [.NET QQSingleActiveNotification example](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/blob/main/docs/Examples/QQSingleActiveNotification/Program.cs).
- [JMS queue](jms_queue) - Same flow as getting started, using `JMSQueueSpecification` (Tanzu RabbitMQ 4.3+).
- [Delayed queue](delayed_queue) - Same flow as getting started, using `DelayedQueueSpecification` (Tanzu RabbitMQ 4.x).
- [Reliable](reliable) - An example of how to deal with reconnections and error handling.
- [Streams](streams) - An example of how to use [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams) with AMQP 1.0
- [Stream Filtering](streams_filtering) - An example of how to use streams [Filter Expressions](https://www.rabbitmq.com/blog/2024/12/13/amqp-filter-expressions)
- [Publisher per message target](publisher_msg_targets) - An example of how to use a single publisher to send messages in different queues with the address to the message target in the message properties.
- [Video](video) - From the YouTube tutorial [AMQP 1.0 with Golang](https://youtu.be/iR1JUFh3udI)
- [TLS](tls) - An example of how to use TLS with the AMQP 1.0 client.
- [Advanced Settings](advanced_settings) - An example of how to use the advanced connection settings of the AMQP 1.0 client.
- [Broadcast](broadcast) - An example of how to use fanout to broadcast messages to multiple auto-deleted queues.
- [RPC Echo](rpc_echo_server) - An example of how to implement RPC with the AMQP 1.0 client.
- [SQL stream Filtering](sql_stream_filter) - An example of how to use SQL stream filtering with RabbitMQ Streams.
- [Web Sockets](web_sockets) - An example of how to use Web Sockets with the AMQP 1.0 client.
- [Pre-settled messages](pre_settled) - An example of how to receive pre-settled messages with the AMQP 1.0 client.
- [Reject a message with a Reason](rejected_by_reason) - An example of reject with rejected-by and rejection reason (RabbitMQ 4.3+) 
- [Quorum Queue Delayed Retry](qq_delayed_retry) - An example of redelivery on quorum queues (RabbitMQ 4.3+)
- [Quorum Queue Delayed Retry - per-message delivery time](qq_delayed_retry_delivery_time) - An example of how to override the redelivery time per message using the `x-opt-delivery-time` annotation (RabbitMQ 4.3+)
- [Quorum Queue Consumer Timeout](qq_consumer_timeout) - An example of quorum queue consumer timeouts (`x-consumer-timeout`) and how to handle the `OnDeliveryRelease` callback (RabbitMQ 4.3+)
- [Publish Async](publish_async) - An example of how to use `PublishAsync` to send messages without blocking the caller while waiting for broker confirmation.
- [OpenTelemetry Metrics](otel_metrics) - An example of how to configure OpenTelemetry metrics with a stdout exporter, showing connection, publisher, consumer, and message counts.
