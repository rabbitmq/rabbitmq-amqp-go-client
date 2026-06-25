// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates per-message delivery time override using the x-opt-delivery-time
// annotation on a quorum queue with delayed retries (RabbitMQ 4.3+).
// and linear backoff strategy using DeliveryMin and DeliveryMax time
// with RequeueWithAnnotationsAndDeliveryFailed(..,true/false).
//
// This is useful for entity-specific rate limits. For example, if an API rejects a request
// for Tenant A with an HTTP 429 and a Retry-After header.
//
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_delayed_retry_delivery_time/main.go

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	queueName := "qq-delayed-retry-delivery-time-go-queue"

	rmq.Info("Quorum queue delayed retry with linear backoff pattern and x-opt-delivery-time override example")

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
	// DelayedRetryMin sets the default linear back-off base delay.
	// The x-opt-delivery-time
	// annotation used below overrides this on a per-message basis.
	const timeMin time.Duration = 1 * time.Second
	const timeMax time.Duration = 10 * time.Second
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name:             queueName,
		DelayedRetryType: rmq.QuorumQueueDelayedRetryFailed,
		DelayedRetryMin:  timeMin,
		DelayedRetryMax:  timeMax,
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

			if retryCount.Add(1) == 1 {
				/// first time we use DelayRetry to override the queue's parameters delay configuration
				err := deliveryContext.DelayRetry(ctx, 2*time.Second, true)
				if err != nil {
					rmq.Error("[Consumer] Error delaying retry", "error", err)
					return
				}
				rmq.Warn("[Consumer] Simulating processing failure, message will be retried with delay 2s", "message", body)
				continue
			}

			if retryCount.Load() < 5 {
				// The API returned HTTP 429 with a Retry-After for this tenant.
				// Here we use the linear backoff patter with  delayed-retry-min/max
				// server side it is calculated with:
				// delay time is min(delayed-retry-min * delivery-count, delayed-retry-max)
				// see: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries
				x := float64(timeMin.Milliseconds() * int64(msg.Header.DeliveryCount+1))
				nextDelivery := math.Min(x, float64(timeMax.Milliseconds()))
				rmq.Warn("[Consumer] Rate-limited,",
					"delivery count", msg.Header.DeliveryCount,
					"retry_after_s", nextDelivery/1000,
					"message", body,
				)
				// modified{delivery-failed=false, undeliverable-here=true}.
				err := deliveryContext.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, true)
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

	for i := range 1 {
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
	<-accepted

	rmq.Info("Normal messages processed. Rate-limited message is set aside by the broker.",
		"retries_triggered", retryCount.Load())
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
