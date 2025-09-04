package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func ptr[T any](v T) *T {
	return &v
}

func main() {
	// see: https://www.rabbitmq.com/docs/next/stream-filtering#sql-filter-expressions
	queueName := "stream-sql-filter-example"

	rmq.Info("AMQP 1.0 Client SQL Stream Filter Example")

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}

	_, err = amqpConnection.Management().DeclareQueue(context.Background(), &rmq.StreamQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Error declaring stream queue", err)
		return
	}

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	// this message will be stored on the queue but not received by the consumer
	// because it does not match the filter
	msgNotIntTheFilter := amqp.NewMessage([]byte("Message that does not match the filter"))
	msgNotIntTheFilter.Properties = &amqp.MessageProperties{
		Subject: ptr("No"),
	}
	msgNotIntTheFilter.ApplicationProperties = map[string]interface{}{"keyNo": "valueNO"}

	pr, err := publisher.Publish(context.Background(), msgNotIntTheFilter)
	if err != nil {
		rmq.Error("Error publishing message", err)
		return
	}
	rmq.Info("Published message that does not match the filter", "publish result", pr.Outcome)

	// this message will be stored on the queue and received by the consumer
	// because it matches the filter
	msgInTheFilter := amqp.NewMessage([]byte("Message that matches the filter"))
	msgInTheFilter.Properties = &amqp.MessageProperties{
		Subject: ptr("Yes_I_am_in_the_filter"),
		To:      ptr("the_id"),
	}
	msgInTheFilter.ApplicationProperties = map[string]interface{}{"keyYes": "valueYES"}

	pr, err = publisher.Publish(context.Background(), msgInTheFilter)
	if err != nil {
		rmq.Error("Error publishing message", err)
		return
	}
	rmq.Info("Published message that matches the filter", "publish result", pr.Outcome)

	consumer, err := amqpConnection.NewConsumer(context.Background(), queueName, &rmq.StreamConsumerOptions{
		InitialCredits: 10,
		Offset:         &rmq.OffsetFirst{},
		StreamFilterOptions: &rmq.StreamFilterOptions{
			// the SQL expression to filter messages
			SQL: "properties.subject LIKE '%Yes_I_am_in_the_filter%' AND properties.to = 'the_id' AND keyYes = 'valueYES'"},
	})

	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consume messages from the queue. It should only receive the message that matches the filter
	// the second
	deliveryContext, err := consumer.Receive(consumerContext)
	if err != nil {
		rmq.Error("[Consumer]", "Error receiving message", err)
		return
	}
	rmq.Info("[Consumer]", "Body",
		fmt.Sprintf("%s", deliveryContext.Message().Data),
		"Subject", *deliveryContext.Message().Properties.Subject, "To", *deliveryContext.Message().Properties.To)
	err = deliveryContext.Accept(context.Background())
	if err != nil {
		rmq.Error("Error accepting message", err)
		return
	}

	err = amqpConnection.Management().DeleteQueue(context.Background(), queueName)
	if err != nil {
		rmq.Error("Error deleting stream queue", err)
		return
	}
	err = amqpConnection.Close(context.Background())
	if err != nil {
		rmq.Error("Error closing connection", err)
		return
	}
	_ = env.CloseConnections(context.Background())
	rmq.Info("AMQP 1.0 Client SQL Stream Filter Example Completed")

}
