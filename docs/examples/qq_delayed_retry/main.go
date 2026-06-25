// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates quorum queue delayed retry (linear back-off redelivery),
// available as of RabbitMQ 4.3. The three queue arguments x-delayed-retry-type,
// x-delayed-retry-min, and x-delayed-retry-max are set via QuorumQueueSpecification.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_delayed_retry/main.go

package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "qq-delayed-retry-go-queue"

	rmq.Info("Quorum queue delayed retry example with AMQP Go AMQP 1.0 Client")

	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for statusChanged := range ch {
			rmq.Info("[connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	amqpConnection, err := env.NewConnection(context.TODO())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	amqpConnection.NotifyStatusChange(stateChanged)

	rmq.Info("AMQP connection opened")
	management := amqpConnection.Management()

	// Declare a quorum queue with delayed retry (requires RabbitMQ 4.3+).
	// - DelayedRetryType: controls which messages trigger linear back-off redelivery.
	//   Use QuorumQueueDelayedRetryReturned so only messages returned (nacked) by
	//   consumers are retried with a delay, rather than all redeliveries.
	// - DelayedRetryMin: minimum back-off delay before the first retry. (valid for Failure)
	// - DelayedRetryMax: upper bound for the back-off delay.
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: queueName,
		//DeliveryLimit:    5,
		DelayedRetryType: rmq.QuorumQueueDelayedRetryReturned,
		DelayedRetryMin:  2 * time.Second,
		//DelayedRetryMax:  30 * time.Second,
	})
	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}
	rmq.Info("Queue declared", "name", queueInfo.Name())

	consumer, err := amqpConnection.NewConsumer(context.TODO(), queueName, nil)
	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.TODO())

	received := make(chan struct{}, 10)
	counter := int32(0)
	go func(ctx context.Context) {
		for {
			deliveryContext, err := consumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				rmq.Info("[Consumer] Consumer closed")
				return
			}
			if err != nil {
				rmq.Error("[Consumer] Error receiving message", "error", err)
				return
			}

			if atomic.AddInt32(&counter, 1) < 10 {
				rmq.Warn("[Consumer] Simulating processing failure, message will be retried with delay 2s", "message",
					fmt.Sprintf("%s", deliveryContext.Message().Data))
				// RequeueWithAnnotationsAndDeliveryFailed with deliveryFailed=false triggers the
				// queue-level configured via QuorumQueueDelayedRetryReturned
				// (modified{delivery-failed=false, undeliverable-here=false}).
				err = deliveryContext.RequeueWithAnnotationsAndDeliveryFailed(context.TODO(), nil, false)
				if err != nil {
					rmq.Error("[Consumer] Error requeuing message", "error", err)
					return
				}
			} else {
				rmq.Info("[Consumer] Received message", "message",
					fmt.Sprintf("%s", deliveryContext.Message().Data))

				err = deliveryContext.Accept(context.TODO())
				if err != nil {
					rmq.Error("[Consumer] Error accepting message", "error", err)
					return
				}
				received <- struct{}{}
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueName}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	const msgCount = 5
	for i := 0; i < msgCount; i++ {
		publishResult, err := publisher.Publish(context.TODO(),
			rmq.NewMessage([]byte(fmt.Sprintf("Hello delayed-retry #%d", i))))
		if err != nil {
			rmq.Error("Error publishing message", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher] Message accepted", "index", i)
		case *rmq.StateReleased:
			rmq.Warn("[Publisher] Message was not routed", "index", i)
		case *rmq.StateRejected:
			rmq.Warn("[Publisher] Message rejected", "index", i)
		default:
			rmq.Warn("[Publisher] Unknown outcome", "outcome", publishResult.Outcome)
		}
	}

	// Wait until all messages are consumed.
	for i := 0; i < msgCount; i++ {
		<-received
	}

	cancel()
	err = consumer.Close(context.TODO())
	if err != nil {
		rmq.Error("[Consumer] Error closing consumer", err)
		return
	}

	err = publisher.Close(context.TODO())
	if err != nil {
		rmq.Error("[Publisher] Error closing publisher", err)
		return
	}

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error deleting queue", "error", err)
		return
	}

	err = env.CloseConnections(context.TODO())
	if err != nil {
		rmq.Error("Error closing connection", "error", err)
		return
	}

	rmq.Info("AMQP connection closed")
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
