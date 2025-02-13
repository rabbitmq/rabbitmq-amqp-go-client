package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmq_amqp"
	"time"
)

func main() {
	exchangeName := "getting-started-go-exchange"
	queueName := "getting-started-go-queue"
	routingKey := "routing-key"

	rabbitmq_amqp.Info("Getting started with AMQP Go AMQP 1.0 Client")

	/// Create a channel to receive state change notifications
	stateChanged := make(chan *rabbitmq_amqp.StateChanged, 1)
	go func(ch chan *rabbitmq_amqp.StateChanged) {
		for statusChanged := range ch {
			rabbitmq_amqp.Info("[connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	// rabbitmq_amqp.NewEnvironment setups the environment.
	// The environment is used to create connections
	// given the same parameters
	env := rabbitmq_amqp.NewEnvironment([]string{"amqp://"}, nil)

	// Open a connection to the AMQP 1.0 server ( RabbitMQ >= 4.0)
	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rabbitmq_amqp.Error("Error opening connection", err)
		return
	}
	// Register the channel to receive status change notifications
	// this is valid for the connection lifecycle
	amqpConnection.NotifyStatusChange(stateChanged)

	rabbitmq_amqp.Info("AMQP connection opened.\n")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &rabbitmq_amqp.TopicExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rabbitmq_amqp.Error("Error declaring exchange", err)
		return
	}

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rabbitmq_amqp.QuorumQueueSpecification{
		Name: queueName,
	})

	if err != nil {
		rabbitmq_amqp.Error("Error declaring queue", err)
		return
	}

	// Bind the queue to the exchange
	bindingPath, err := management.Bind(context.TODO(), &rabbitmq_amqp.ExchangeToQueueBindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})

	if err != nil {
		rabbitmq_amqp.Error("Error binding", err)
		return
	}

	// Create a consumer to receive messages from the queue
	// you need to build the address of the queue, but you can use the helper function

	consumer, err := amqpConnection.NewConsumer(context.Background(), queueName, nil)
	if err != nil {
		rabbitmq_amqp.Error("Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.Background())

	// Consume messages from the queue
	go func(ctx context.Context) {
		for {
			deliveryContext, err := consumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				// The consumer was closed correctly
				rabbitmq_amqp.Info("[NewConsumer]", "consumer closed. Context", err)
				return
			}
			if err != nil {
				// An error occurred receiving the message
				rabbitmq_amqp.Error("[NewConsumer]", "Error receiving message", err)
				return
			}

			rabbitmq_amqp.Info("[NewConsumer]", "Received message",
				fmt.Sprintf("%s", deliveryContext.Message().Data))

			err = deliveryContext.Accept(context.Background())
			if err != nil {
				rabbitmq_amqp.Error("Error accepting message", err)
				return
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rabbitmq_amqp.ExchangeAddress{
		Exchange: exchangeName,
		Key:      routingKey,
	}, "getting-started-publisher")
	if err != nil {
		rabbitmq_amqp.Error("Error creating publisher", err)
		return
	}

	for i := 0; i < 100; i++ {
		// Publish a message to the exchange
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!"+fmt.Sprintf("%d", i))))
		if err != nil {
			rabbitmq_amqp.Error("Error publishing message", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *amqp.StateAccepted:
			rabbitmq_amqp.Info("[NewPublisher]", "Message accepted", publishResult.Message.Data[0])
			break
		case *amqp.StateReleased:
			rabbitmq_amqp.Warn("[NewPublisher]", "Message was not routed", publishResult.Message.Data[0])
			break
		case *amqp.StateRejected:
			rabbitmq_amqp.Warn("[NewPublisher]", "Message rejected", publishResult.Message.Data[0])
			stateType := publishResult.Outcome.(*amqp.StateRejected)
			if stateType.Error != nil {
				rabbitmq_amqp.Warn("[NewPublisher]", "Message rejected with error: %v", stateType.Error)
			}
			break
		default:
			// these status are not supported. Leave it for AMQP 1.0 compatibility
			// see: https://www.rabbitmq.com/docs/next/amqp#outcomes
			rabbitmq_amqp.Warn("Message state: %v", publishResult.Outcome)
		}
	}

	println("press any key to close the connection")

	var input string
	_, _ = fmt.Scanln(&input)

	cancel()
	//Close the consumer
	err = consumer.Close(context.Background())
	if err != nil {
		rabbitmq_amqp.Error("[NewConsumer]", err)
		return
	}
	// Close the publisher
	err = publisher.Close(context.Background())
	if err != nil {
		rabbitmq_amqp.Error("[NewPublisher]", err)
		return
	}

	// Unbind the queue from the exchange
	err = management.Unbind(context.TODO(), bindingPath)

	if err != nil {
		rabbitmq_amqp.Error("Error unbinding: %v\n", err)
		return
	}

	err = management.DeleteExchange(context.TODO(), exchangeInfo.Name())
	if err != nil {
		rabbitmq_amqp.Error("Error deleting exchange: %v\n", err)
		return
	}

	// Purge the queue
	purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rabbitmq_amqp.Error("Error purging queue: %v\n", err)
		return
	}
	rabbitmq_amqp.Info("Purged %d messages from the queue.\n", purged)

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rabbitmq_amqp.Error("Error deleting queue: %v\n", err)
		return
	}

	// Close all the connections. but you can still use the environment
	// to create new connections
	err = env.CloseConnections(context.Background())
	if err != nil {
		rabbitmq_amqp.Error("Error closing connection: %v\n", err)
		return
	}

	rabbitmq_amqp.Info("AMQP connection closed.\n")
	// not necessary. It waits for the status change to be printed
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
