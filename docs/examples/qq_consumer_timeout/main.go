// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates quorum queue consumer timeouts (x-consumer-timeout).
//
// Consumer timeouts limit how long a consumer may hold a message without settling it
// (accept / discard / requeue). When the timer fires, the broker sends a DISPOSITION
// frame with Released state to the consumer, which triggers the OnDeliveryRelease callback.
//
// There are two levels of consumer timeout:
//   - Queue-level: x-consumer-timeout queue argument (via QuorumQueueSpecification.ConsumerTimeout)
//   - Consumer-level: rabbitmq:consumer-timeout AMQP attach property (via ConsumerOptions.ConsumerTimeout)
//
// The per-consumer setting takes precedence when both are configured.
//
// Inside OnDeliveryRelease, calling Accept() sends an AMQP Accepted disposition back to the
// broker, unlocking the consumer so it can continue receiving messages. Discard and Requeue
// are not valid in this context and will return ErrDeliveryReleaseInvalidOperation.
//
// Requires RabbitMQ 4.3 or later.
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#consumer-timeouts
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_consumer_timeout/main.go

package main

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "qq-consumer-timeout-go-example"

	rmq.Info("Quorum queue consumer timeout example (RabbitMQ 4.3+)")

	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for statusChanged := range ch {
			rmq.Info("[connection]", "status changed", statusChanged)
		}
	}(stateChanged)

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	conn, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	conn.NotifyStatusChange(stateChanged)
	defer func() {
		_ = env.CloseConnections(context.Background())
		close(stateChanged)
	}()

	rmq.Info("AMQP connection opened")
	management := conn.Management()

	// 1. Declare a quorum queue with a queue-level consumer timeout of 5 seconds.
	//    This means any consumer that holds a message for more than 5 s will be timed out.
	consumerTimeout := 5 * time.Second
	queueInfo, err := management.DeclareQueue(context.Background(), &rmq.QuorumQueueSpecification{
		Name:            queueName,
		ConsumerTimeout: consumerTimeout,
	})
	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}
	rmq.Info("Queue declared", "name", queueInfo.Name(), "x-consumer-timeout", consumerTimeout)

	// 2. Publish a single message that will be used to trigger the timeout scenario.
	publisher, err := conn.NewPublisher(context.Background(), &rmq.QueueAddress{Queue: queueName}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	msg := rmq.NewMessage([]byte("timeout-demo-message"))
	result, err := publisher.Publish(context.Background(), msg)
	if err != nil {
		rmq.Error("Error publishing message", err)
		return
	}
	switch result.Outcome.(type) {
	case *rmq.StateAccepted:
		rmq.Info("[Publisher] Message accepted by broker")
	case *rmq.StateReleased:
		rmq.Warn("[Publisher] Message released (not routed)")
		return
	case *rmq.StateRejected:
		rmq.Error("[Publisher] Message rejected", "outcome", result.Outcome)
		return
	}
	_ = publisher.Close(context.Background())

	// Track whether the OnDeliveryRelease callback fired and the second delivery succeeded.
	var releaseCallbackFired atomic.Bool
	var finalAccept atomic.Bool

	released := make(chan struct{}, 1)
	accepted := make(chan struct{}, 1)

	// 3. Create a consumer with:
	//    - ConsumerTimeout: per-consumer override (takes precedence over queue-level setting)
	//    - OnDeliveryRelease: called when the broker releases the timed-out delivery
	consumer, err := conn.NewConsumer(context.Background(), queueName, &rmq.ConsumerOptions{
		// Override timeout at the per-consumer level (here same as queue-level for clarity).
		ConsumerTimeout: consumerTimeout,

		// OnDeliveryRelease is invoked when the broker releases the delivery after timeout.
		// Only Accept() is valid here – calling Discard or Requeue returns an error.
		OnDeliveryRelease: func(deliveryCtx rmq.IDeliveryContext, message *amqp.Message) {
			rmq.Info("[Consumer] Delivery released by broker (consumer timeout fired)",
				"body", string(message.GetData()))

			releaseCallbackFired.Store(true)

			// Accept the message to unlock the consumer from the timeout state.
			if acceptErr := deliveryCtx.Accept(context.Background()); acceptErr != nil {
				rmq.Error("[Consumer] Failed to accept timed-out delivery", "error", acceptErr)
			} else {
				rmq.Info("[Consumer] Accepted timed-out delivery – consumer unlocked")
			}
			released <- struct{}{}
		},
	})
	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}

	rmq.Info("[Consumer] Attached with consumer timeout", "timeout", consumerTimeout)

	consumerCtx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			deliveryCtx, recvErr := consumer.Receive(ctx)
			if errors.Is(recvErr, context.Canceled) {
				rmq.Info("[Consumer] Consumer context cancelled, exiting receive loop")
				return
			}
			if recvErr != nil {
				rmq.Error("[Consumer] Error receiving message", "error", recvErr)
				return
			}

			msg := deliveryCtx.Message()
			rmq.Info("[Consumer] Message received – waiting past timeout to trigger OnDeliveryRelease",
				"body", string(msg.GetData()))

			// Deliberately hold the message for longer than the consumer timeout so the broker
			// fires the consumer-timeout mechanism and triggers OnDeliveryRelease above.
			// After OnDeliveryRelease calls Accept(), the consumer is unlocked and the requeued
			// message is redelivered – we accept it here on the second delivery.
			time.Sleep(consumerTimeout + 3*time.Second)

			acceptErr := deliveryCtx.Accept(ctx)
			if acceptErr != nil {
				// Expected on the first delivery: the broker has already released it.
				rmq.Warn("[Consumer] Accept on first delivery failed (expected after timeout)",
					"error", acceptErr)
			} else {
				// Second delivery (after unlock): Accept succeeds.
				rmq.Info("[Consumer] Message accepted on second delivery",
					"body", string(msg.GetData()))
				finalAccept.Store(true)
				accepted <- struct{}{}
			}
		}
	}(consumerCtx)

	// Wait for the release callback to fire (broker releases due to timeout).
	select {
	case <-released:
		rmq.Info("OnDeliveryRelease callback fired successfully")
	case <-time.After(30 * time.Second):
		rmq.Error("Timed out waiting for OnDeliveryRelease callback")
	}

	// Wait for the redelivered message to be accepted.
	select {
	case <-accepted:
		rmq.Info("Redelivered message accepted successfully")
	case <-time.After(30 * time.Second):
		rmq.Error("Timed out waiting for redelivered message acceptance")
	}

	cancel()
	_ = consumer.Close(context.Background())

	_ = management.DeleteQueue(context.Background(), queueName)
	rmq.Info("Example completed", "release_callback_fired", releaseCallbackFired.Load(), "final_accept", finalAccept.Load())
}
