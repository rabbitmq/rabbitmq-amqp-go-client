// Package rabbitmqamqp provides a Go client for RabbitMQ using the AMQP 1.0 protocol.
//
// This package wraps the [Azure go-amqp] client and offers high-level abstractions
// for common RabbitMQ operations: publishing and consuming messages, managing connections and sessions, and handling errors.
// This package provides an easy way to use the RabbitMQ 4.x features, like stream filtering, single active consumer,
// websockets, reject with reason, and many more.
// It also provides the auto-reconnection feature, which allows the client to automatically
// reconnect to the server in case of a connection failure.
//
// # Getting Started
//
// Create a connection, declare a queue, publish a message, and consume it:
//
//	conn, err := rabbitmqamqp.Dial(context.Background(), "amqp://guest:guest@localhost/", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close(context.Background())
//
//	mgmt := conn.Management()
//	_, err = mgmt.DeclareQueue(context.Background(), &rabbitmqamqp.QuorumQueueSpecification{Name: "my-queue"})
//
//	publisher, _ := conn.NewPublisher(context.Background(), &rabbitmqamqp.QueueAddress{Queue: "my-queue"}, nil)
//	publisher.Publish(context.Background(), rabbitmqamqp.NewMessage([]byte("Hello, RabbitMQ!")))
//
//	consumer, _ := conn.NewConsumer(context.Background(), "my-queue", nil)
//	delivery, _ := consumer.Receive(context.Background())
//	delivery.Accept(context.Background())
//
// # Examples
//
// Ready-to-run examples for common use cases are available in the
// [docs/examples] directory of the repository:
//
//   - [Getting Started] — A simple example to get started.
//   - [Reliable] — Reconnections and error handling.
//   - [Streams] — Using [RabbitMQ Streams] with AMQP 1.0.
//   - [Stream Filtering] — Using AMQP 1.0 [filter expressions].
//   - [SQL Stream Filtering] — SQL filter expressions on streams.
//   - [RPC Echo] — Implementing RPC (request/reply) over AMQP 1.0.
//   - [TLS] — Connecting with TLS.
//   - [Advanced Settings] — Advanced connection settings.
//   - [Broadcast] — Fanout exchange to broadcast to multiple queues.
//   - [Publisher per Message Target] — Single publisher, multiple queue targets.
//   - [Pre-Settled Messages] — Receiving pre-settled (fire-and-forget) messages.
//   - [Publish Async] — Asynchronous publishing with callbacks.
//   - [Web Sockets] — Connecting over WebSocket.
//   - [OpenTelemetry Metrics] — Collecting metrics with OpenTelemetry.
//   - [Reject with Reason] — Rejecting messages with a reason (RabbitMQ 4.3+).
//   - [Quorum Queue Delayed Retry] — Delayed redelivery on quorum queues (RabbitMQ 4.3+).
//   - [Quorum Queue Delayed Retry per Message] — Per-message delivery time override (RabbitMQ 4.3+).
//   - [Quorum Queue SAC Notification] — Single-active-consumer state notifications (RabbitMQ 4.3+).
//   - [JMS Queue] — Using JMS queues (Tanzu RabbitMQ 4.3+).
//   - [Delayed Queue] — Using delayed queues (Tanzu RabbitMQ 4.x+).
//   - [Video] — Companion code for the [AMQP 1.0 with Golang] YouTube tutorial.
//
// [Azure go-amqp]: https://github.com/Azure/go-amqp
// [docs/examples]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples
// [Getting Started]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/getting_started
// [Reliable]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/reliable
// [Streams]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/streams
// [RabbitMQ Streams]: https://www.rabbitmq.com/docs/streams
// [Stream Filtering]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/streams_filtering
// [filter expressions]: https://www.rabbitmq.com/blog/2024/12/13/amqp-filter-expressions
// [SQL Stream Filtering]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/sql_stream_filter
// [RPC Echo]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/rpc_echo_server
// [TLS]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/tls
// [Advanced Settings]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/advanced_settings
// [Broadcast]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/broadcast
// [Publisher per Message Target]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/publisher_msg_targets
// [Pre-Settled Messages]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/pre_settled
// [Publish Async]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/publish_async
// [Web Sockets]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/web_sockets
// [OpenTelemetry Metrics]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/otel_metrics
// [Reject with Reason]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/rejected_by_reason
// [Quorum Queue Delayed Retry]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_delayed_retry
// [Quorum Queue Delayed Retry per Message]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_delayed_retry_delivery_time
// [Quorum Queue SAC Notification]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_single_active_notification
// [JMS Queue]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/jms_queue
// [Delayed Queue]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/delayed_queue
// [Video]: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/video
// [AMQP 1.0 with Golang]: https://youtu.be/iR1JUFh3udI
package rabbitmqamqp
