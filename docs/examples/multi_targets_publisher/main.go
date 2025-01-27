package main

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

func validateOutcome(outcome amqp.DeliveryState) {

	switch outcome.(type) {
	case *amqp.StateAccepted:
		rabbitmq_amqp.Info("Message accepted")
	default:
		rabbitmq_amqp.Info("Message not accepted")
		panic("Message not accepted")
	}

}

func main() {

	rabbitmq_amqp.Info("Multi Target Publisher with AMQP Go AMQP 1.0 Client")
	queueName1 := "multi-target-queue-1"
	queueName2 := "multi-target-queue-2"

	connection, err := rabbitmq_amqp.Dial(context.Background(), []string{"amqp://"}, nil)
	if err != nil {
		return
	}

	_, err = connection.Management().DeclareQueue(context.Background(), &rabbitmq_amqp.QueueSpecification{
		Name:      queueName1,
		QueueType: rabbitmq_amqp.QueueType{Type: rabbitmq_amqp.Quorum},
	})
	if err != nil {
		return
	}

	_, err = connection.Management().DeclareQueue(context.Background(), &rabbitmq_amqp.QueueSpecification{
		Name:      queueName2,
		QueueType: rabbitmq_amqp.QueueType{Type: rabbitmq_amqp.Classic},
	})

	if err != nil {
		return
	}

	// Create a publisher that can publish to multiple targets
	mtPublisher, err := connection.NewMultiTargetsPublisher(context.Background(), "test")
	if err != nil {
		return
	}
	t1, _ := rabbitmq_amqp.QueueAddress(&queueName1)
	t2, _ := rabbitmq_amqp.QueueAddress(&queueName2)

	publishResult, err := mtPublisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), t1)
	if err != nil {
		return
	}

	validateOutcome(publishResult.Outcome)

	publishResult, err = mtPublisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), t2)
	if err != nil {
		return
	}

	validateOutcome(publishResult.Outcome)

	// Close the publisher
	err = mtPublisher.Close(context.Background())
	if err != nil {
		return
	}

	// Delete the queues
	err = connection.Management().DeleteQueue(context.Background(), queueName1)
	if err != nil {
		return
	}

	err = connection.Management().DeleteQueue(context.Background(), queueName2)
	if err != nil {
		return
	}

	// Close the connection

	err = connection.Close(context.Background())
	if err != nil {
		return
	}

}
