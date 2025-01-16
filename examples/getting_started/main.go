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

	fmt.Printf("Getting started with AMQP Go AMQP 1.0 Client\n")

	/// Create a channel to receive status change notifications
	chStatusChanged := make(chan *rabbitmq_amqp.StatusChanged, 1)
	go func(ch chan *rabbitmq_amqp.StatusChanged) {
		for statusChanged := range ch {
			fmt.Printf("%s\n", statusChanged)
		}
	}(chStatusChanged)

	// Open a connection to the AMQP 1.0 server
	amqpConnection, err := rabbitmq_amqp.Dial(context.Background(), []string{"amqp://"}, nil)
	if err != nil {
		fmt.Printf("Error opening connection: %v\n", err)
		return
	}
	// Register the channel to receive status change notifications
	amqpConnection.NotifyStatusChange(chStatusChanged)

	fmt.Printf("AMQP Connection opened.\n")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &rabbitmq_amqp.ExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		fmt.Printf("Error declaring exchange: %v\n", err)
		return
	}

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rabbitmq_amqp.QueueSpecification{
		Name:      queueName,
		QueueType: rabbitmq_amqp.QueueType{Type: rabbitmq_amqp.Quorum},
	})

	if err != nil {
		fmt.Printf("Error declaring queue: %v\n", err)
		return
	}

	// Bind the queue to the exchange
	bindingPath, err := management.Bind(context.TODO(), &rabbitmq_amqp.BindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})

	if err != nil {
		fmt.Printf("Error binding: %v\n", err)
		return
	}

	addr, err := rabbitmq_amqp.ExchangeAddress(&exchangeName, &routingKey)

	publisher, err := amqpConnection.Publisher(context.Background(), addr, "getting-started-publisher")
	if err != nil {
		fmt.Printf("Error creating publisher: %v\n", err)
		return
	}

	// Publish a message to the exchange
	outcome, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!")))
	if err != nil {
		fmt.Printf("Error publishing message: %v\n", err)
		return
	}

	// determine how the peer settled the message
	switch stateType := outcome.DeliveryState.(type) {
	case *amqp.StateAccepted:
		// message was accepted, no further action is required
		rabbitmq_amqp.Info("Message accepted")
	case *amqp.StateModified:
		// message must be modified and resent before it can be processed.
		// the values in stateType provide further context.
		rabbitmq_amqp.Info("Message modified")
	case *amqp.StateReceived:
		// see the fields in [StateReceived] for information on
		// how to handle this delivery state.
		rabbitmq_amqp.Info("Message received")
	case *amqp.StateRejected:
		rabbitmq_amqp.Warn("Message rejected")
		// the peer rejected the message
		if stateType.Error != nil {
			// the error will provide information about why the
			// message was rejected. note that the peer isn't required
			// to provide an error.
			rabbitmq_amqp.Warn("Message rejected with error: %v", stateType.Error)
		}
	case *amqp.StateReleased:
		// message was not routed
		rabbitmq_amqp.Warn("Message was not routed")
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

	close(chStatusChanged)
}
