// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example is demonstrating how to consume messages with pre-settled mode enabled.
// pre-settled is valid only for classic and quorum queues.
// Pre-settled mode means that messages are considered accepted by the broker as soon as they are delivered to the consumer,
// without requiring explicit acceptance from the consumer side.
// This is useful for scenarios where at-most-once delivery semantics are acceptable, and it can improve performance by reducing
// the overhead of message acknowledgments.
// Example path:https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/pre_settled/pre_settled.go
package main

import (
	"context"
	"fmt"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	rmq.Info("Pre-Settled example with AMQP Go AMQP 1.0 Client")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	amqpConnection, err := env.NewConnection(context.TODO())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	// create queue for the example
	_, err = amqpConnection.Management().DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: "pre-settled-queue",
	})
	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}
	rmq.Info("Queue 'pre-settled-queue' declared")

	// publish some messages to the queue
	producer, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{
		Queue: "pre-settled-queue",
	}, nil)
	if err != nil {
		rmq.Error("Error creating producer", err)
		return
	}
	for i := 0; i < 100; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("Pre-settled message number %d ", i+1)))
		_, err := producer.Publish(context.TODO(), msg)
		if err != nil {
			rmq.Error("Error publishing message", err)
			return
		}
	}

	// create consumer with pre-settled mode enabled

	consumer, err := amqpConnection.NewConsumer(context.TODO(), "pre-settled-queue", &rmq.ConsumerOptions{
		PreSettled: true,
	})
	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}
	rmq.Info("Consumer created with pre-settled mode enabled")

	for i := 0; i < 100; i++ {
		dc, err := consumer.Receive(context.TODO())
		if err != nil {
			rmq.Error("Error consuming message", err)
			return
		}
		// here we don't need to accept the message
		// because pre-settled mode is enabled
		rmq.Info("[Consumer]", "Message received", string(dc.Message().GetData()))
	}

	// clean up
	err = consumer.Close(context.TODO())
	if err != nil {
		rmq.Error("Error closing consumer", err)
		return
	}
	err = producer.Close(context.TODO())
	if err != nil {
		rmq.Error("Error closing producer", err)
		return
	}
	// delete the queue
	err = amqpConnection.Management().DeleteQueue(context.TODO(), "pre-settled-queue")
	if err != nil {
		rmq.Error("Error deleting queue", err)
		return
	}
	rmq.Info("Queue 'pre-settled-queue' deleted")

	err = amqpConnection.Close(context.TODO())
	if err != nil {
		rmq.Error("Error closing connection", err)
		return
	}
	rmq.Info("Example finished successfully")

}
