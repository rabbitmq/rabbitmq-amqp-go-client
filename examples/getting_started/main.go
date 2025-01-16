package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
	"time"
)

func main() {
	exchangeName := "getting-started-exchange"
	queueName := "getting-started-queue"
	routingKey := "routing-key"

	rabbitmq_amqp.Info("Getting started with AMQP Go AMQP 1.0 Client")

	/// Create a channel to receive state change notifications
	stateChanged := make(chan *rabbitmq_amqp.StateChanged, 1)
	go func(ch chan *rabbitmq_amqp.StateChanged) {
		for statusChanged := range ch {
			rabbitmq_amqp.Info("[Connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	// Open a connection to the AMQP 1.0 server
	amqpConnection, err := rabbitmq_amqp.Dial(context.Background(), []string{"amqp://"}, nil)
	if err != nil {
		rabbitmq_amqp.Error("Error opening connection", err)
		return
	}
	// Register the channel to receive status change notifications
	amqpConnection.NotifyStatusChange(stateChanged)

	fmt.Printf("AMQP Connection opened.\n")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &rabbitmq_amqp.ExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rabbitmq_amqp.Error("Error declaring exchange", err)
		return
	}

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rabbitmq_amqp.QueueSpecification{
		Name:      queueName,
		QueueType: rabbitmq_amqp.QueueType{Type: rabbitmq_amqp.Quorum},
	})

	if err != nil {
		rabbitmq_amqp.Error("Error declaring queue", err)
		return
	}

	// Bind the queue to the exchange
	bindingPath, err := management.Bind(context.TODO(), &rabbitmq_amqp.BindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})

	if err != nil {
		rabbitmq_amqp.Error("Error binding", err)
		return
	}

	addr, err := rabbitmq_amqp.ExchangeAddress(&exchangeName, &routingKey)

	publisher, err := amqpConnection.Publisher(context.Background(), addr, "getting-started-publisher")
	if err != nil {
		rabbitmq_amqp.Error("Error creating publisher", err)
		return
	}

	// Publish a message to the exchange
	publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!")))
	if err != nil {
		rabbitmq_amqp.Error("Error publishing message", err)
		return
	}
	switch publishResult.Outcome {
	case &amqp.StateAccepted{}:
		rabbitmq_amqp.Info("Message accepted")
	case &amqp.StateReleased{}:
		rabbitmq_amqp.Warn("Message was not routed")
	case &amqp.StateRejected{}:
		rabbitmq_amqp.Warn("Message rejected")
		stateType := publishResult.Outcome.(*amqp.StateRejected)
		if stateType.Error != nil {
			rabbitmq_amqp.Warn("Message rejected with error: %v", stateType.Error)
		}
	default:
		// these status are not supported
		rabbitmq_amqp.Warn("Message state: %v", publishResult.Outcome)
	}

	println("press any key to close the connection")

	var input string
	_, _ = fmt.Scanln(&input)

	// Close the publisher
	err = publisher.Close(context.Background())
	if err != nil {
		return
	}
	// Unbind the queue from the exchange

	err = management.Unbind(context.TODO(), bindingPath)

	if err != nil {
		fmt.Printf("Error unbinding: %v\n", err)
		return
	}

	err = management.DeleteExchange(context.TODO(), exchangeInfo.Name())
	if err != nil {
		fmt.Printf("Error deleting exchange: %v\n", err)
		return
	}

	// Purge the queue
	purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		fmt.Printf("Error purging queue: %v\n", err)
		return
	}
	fmt.Printf("Purged %d messages from the queue.\n", purged)

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		fmt.Printf("Error deleting queue: %v\n", err)
		return
	}

	err = amqpConnection.Close(context.Background())
	if err != nil {
		fmt.Printf("Error closing connection: %v\n", err)
		return
	}

	fmt.Printf("AMQP Connection closed.\n")
	// Wait for the status change to be printed
	time.Sleep(500 * time.Millisecond)

	close(stateChangeds)
}
