// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates how to use the consumer priority feature introduced in RabbitMQ 4.3.
// It allows the broker to prioritize which active consumers receive messages first
// when multiple consumers are attached to the same queue.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/consumer_priority/main.go

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
	queueName := "consumer-priority-go-queue"

	rmq.Info("Consumer priority example with AMQP 1.0 Go Client (requires RabbitMQ 4.3+)")

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

	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}
	rmq.Info("Queue declared", "name", queueInfo.Name())

	// Create a High Priority Consumer. In this example, the high priority consumer has a priority of 10,
	// while the low priority consumer has a priority of 1.
	// and it has to receive all the messages before the low priority consumer can receive any messages.
	highPriorityConsumer, err := amqpConnection.NewConsumer(context.TODO(), queueName, &rmq.ConsumerOptions{
		Priority: &rmq.Priority{Value: 10},
	})
	if err != nil {
		rmq.Error("Error creating high priority consumer", err)
		return
	}
	rmq.Info("High Priority Consumer attached (Priority: 10)")

	// Create a Low Priority Consumer
	lowPriorityConsumer, err := amqpConnection.NewConsumer(context.TODO(), queueName, &rmq.ConsumerOptions{
		Priority: &rmq.Priority{Value: 1},
	})
	if err != nil {
		rmq.Error("Error creating low priority consumer", err)
		return
	}
	rmq.Info("Low Priority Consumer attached (Priority: 1)")

	consumerContext, cancel := context.WithCancel(context.TODO())
	var highCount, lowCount atomic.Int32

	// Start High Priority receive loop
	go func(ctx context.Context) {
		for {
			deliveryContext, err := highPriorityConsumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				return
			}
			highCount.Add(1)
			rmq.Info("[High Priority Consumer]", "Received message", fmt.Sprintf("%s", deliveryContext.Message().Data))
			_ = deliveryContext.Accept(context.TODO())
		}
	}(consumerContext)

	// Start Low Priority receive loop
	go func(ctx context.Context) {
		for {
			deliveryContext, err := lowPriorityConsumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				return
			}
			lowCount.Add(1)
			rmq.Info("[Low Priority Consumer]", "Received message", fmt.Sprintf("%s", deliveryContext.Message().Data))
			_ = deliveryContext.Accept(context.TODO())
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueName}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	// Publish messages
	for i := 0; i < 20; i++ {
		_, err := publisher.Publish(context.TODO(), rmq.NewMessage([]byte(fmt.Sprintf("Message #%d", i))))
		if err != nil {
			rmq.Error("Error publishing message", "error", err)
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	rmq.Info("Consumption Results", "High Priority Count", highCount.Load(), "Low Priority Count", lowCount.Load())

	cancel()
	_ = highPriorityConsumer.Close(context.TODO())
	_ = lowPriorityConsumer.Close(context.TODO())
	_ = publisher.Close(context.TODO())
	_ = management.DeleteQueue(context.TODO(), queueName)
	_ = env.CloseConnections(context.TODO())

	rmq.Info("AMQP connection closed")
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
