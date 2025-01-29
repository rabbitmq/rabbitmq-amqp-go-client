package main

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

func validateOutcome(outcome amqp.DeliveryState, error error) {
	if error != nil {
		panic(error)
	}

	if outcome != nil {
		switch outcome.(type) {
		case *amqp.StateAccepted:
			rabbitmq_amqp.Info("Message accepted")
		default:
			rabbitmq_amqp.Info("Message not accepted")
			panic("Message not accepted")
		}
	}

}

func main() {

	rabbitmq_amqp.Info("Multi Target Publisher with AMQP Go AMQP 1.0 Client")
	var queuesArray = []string{"multi-target-queue-1", "multi-target-queue-2"}

	connection, err := rabbitmq_amqp.Dial(context.Background(), []string{"amqp://"}, nil)
	if err != nil {
		return
	}

	for _, queue := range queuesArray {
		_, err = connection.Management().DeclareQueue(context.Background(), &rabbitmq_amqp.QuorumQueueSpecification{
			Name: queue,
		})
	}

	// Create a publisher that can publish to multiple targets
	mtPublisher, err := connection.NewMultiTargetsPublisher(context.Background(), "test")
	if err != nil {
		return
	}

	for _, queue := range queuesArray {
		rabbitmq_amqp.Info("[Publisher]", "Publishing to queue", queue)
		publishResult, err := mtPublisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")),
			&rabbitmq_amqp.QueueAddress{
				Queue: queue,
			})

		validateOutcome(publishResult.Outcome, err)
	}

	// Close the publisher
	err = mtPublisher.Close(context.Background())
	if err != nil {
		return
	}
	rabbitmq_amqp.Info("[Publisher] Closed")

	// Delete the queues
	for _, queue := range queuesArray {
		err = connection.Management().DeleteQueue(context.Background(), queue)
		if err != nil {
			return
		}
		rabbitmq_amqp.Info("[Queue]", "Deleted", queue)
	}

	// Close the connection

	err = connection.Close(context.Background())
	if err != nil {
		return
	}
	rabbitmq_amqp.Info("[Connection] Closed")

}
