// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates per-message delivery time override using the x-opt-delivery-time
// annotation on a quorum queue with delayed retries (RabbitMQ 4.3+).
//
// Instead of relying  on  x-delayed-retry-min,
// a consumer can return a message with RequeueWithAnnotations and set the x-opt-delivery-time
// annotation to a Unix timestamp in milliseconds, instructing RabbitMQ to redeliver the
// message at that exact time.
//
// This is useful for entity-specific rate limits. For example, if an API rejects a request
// for Tenant A with an HTTP 429 and a Retry-After header, you can parse that header and
// set x-opt-delivery-time only for Tenant A's message, while Tenant B's messages continue
// to be processed normally.
//
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_delayed_retry_delivery_time/main.go

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "qq-delayed-retry-delivery-time-go-queue"

	rmq.Info("Quorum queue delayed retry with x-opt-delivery-time override example")

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

	// Declare a quorum queue with delayed retry enabled (requires RabbitMQ 4.3+).
	// DelayedRetryMin sets the default linear back-off base delay. The x-opt-delivery-time
	// annotation used below overrides this on a per-message basis.
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name:             queueName,
		DelayedRetryType: rmq.QuorumQueueDelayedRetryFailed,
		DelayedRetryMin:  30 * time.Second,
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

	accepted := make(chan struct{}, 10)
	retryCount := atomic.Int32{}

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

			msg := deliveryContext.Message()
			body := fmt.Sprintf("%s", msg.Data)

			if retryCount.Add(1) < 10 {
				// The API returned HTTP 429 with a Retry-After for this tenant.
				// Override the queue's default delayed retry behavior with a per-message delay.

				random := time.Duration(5+rand.Intn(5)) * time.Second
				retryCount.Add(1)
				rmq.Warn("[Consumer] Rate-limited, overriding delivery time via DelayRetry",
					"retry_after_s", random.Seconds(),
					"message", body,
				)
				// DelayRetry sets x-opt-delivery-time = now+random and sends
				// modified{delivery-failed=false, undeliverable-here=true}.
				err := deliveryContext.DelayRetry(random, true)
				if err != nil {
					return
				}
			} else {
				rmq.Info("[Consumer] Message processed successfully",
					"message", body, "annotations", msg.Annotations,
				)
				acceptErr := deliveryContext.Accept(ctx)
				if acceptErr != nil {
					rmq.Error("[Consumer] Error accepting message", "error", acceptErr)
					return
				}
				accepted <- struct{}{}
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueName}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	for i := range 10 {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("my_message nr:%d", i)))
		publishResult, err := publisher.Publish(context.TODO(), msg)
		if err != nil {
			rmq.Error("Error publishing message", "error", err)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher] Message accepted", "index", i)
		case *rmq.StateReleased:
			rmq.Warn("[Publisher] Message was not routed", "index", i)
		case *rmq.StateRejected:
			rmq.Warn("[Publisher] Message rejected", "index", i)
		}
	}

	// Wait until the two non-rate-limited messages are accepted.

	for i := 0; i < 10; i++ {
		<-accepted
	}

	rmq.Info("Normal messages processed. Rate-limited message is set aside by the broker.",
		"retries_triggered", retryCount.Load(),
		"note", "The rate-limited message will be redelivered after the x-opt-delivery-time elapses (from 5s to 10s from retry)")

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
