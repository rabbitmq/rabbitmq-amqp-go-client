package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	exchangeName := "getting-started-go-exchange"
	queueName := "getting-started-go-queue"
	routingKey := "routing-key"

	rmq.Info("Getting started with AMQP Go AMQP 1.0 Client")

	/// Create a channel to receive connection state change notifications
	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for statusChanged := range ch {
			rmq.Info("[connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	// rmq.NewClusterEnvironment setups the environment.
	// The environment is used to create connections
	// given the same parameters
	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	// in case you have multiple endpoints you can use the following:
	//env := rmq.NewClusterEnvironment([]rmq.Endpoint{
	//	{Address: "amqp://server1", Options: &rmq.AmqpConnOptions{}},
	//	{Address: "amqp://server2", Options: &rmq.AmqpConnOptions{}},
	//})

	// Open a connection to the AMQP 1.0 server ( RabbitMQ >= 4.0)
	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	// Register the channel to receive status change notifications
	// this is valid for the connection lifecycle
	amqpConnection.NotifyStatusChange(stateChanged)

	rmq.Info("AMQP connection opened")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &rmq.TopicExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rmq.Error("Error declaring exchange", err)
		return
	}

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})

	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}

	// Bind the queue to the exchange
	bindingPath, err := management.Bind(context.TODO(), &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})

	if err != nil {
		rmq.Error("Error binding", err)
		return
	}

	// Create a consumer to receive messages from the queue
	// you need to build the address of the queue, but you can use the helper function
	consumer, err := amqpConnection.NewConsumer(context.Background(), queueName, nil)
	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.Background())

	// Consume messages from the queue
	go func(ctx context.Context) {
		for {
			deliveryContext, err := consumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				// The consumer was closed correctly
				rmq.Info("[Consumer] Consumer closed", "context", err)
				return
			}
			if err != nil {
				// An error occurred receiving the message
				rmq.Error("[Consumer] Error receiving message", "error", err)
				return
			}

			rmq.Info("[Consumer] Received message", "message",
				fmt.Sprintf("%s", deliveryContext.Message().Data))

			err = deliveryContext.Accept(context.Background())
			if err != nil {
				rmq.Error("[Consumer] Error accepting message", "error", err)
				return
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rmq.ExchangeAddress{
		Exchange: exchangeName,
		Key:      routingKey,
	}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	for i := 0; i < 100; i++ {
		// Publish a message to the exchange
		publishResult, err := publisher.Publish(context.Background(), rmq.NewMessage([]byte("Hello, World!"+fmt.Sprintf("%d", i))))
		if err != nil {
			rmq.Error("Error publishing message", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher]", "Message accepted", publishResult.Message.Data[0])
		case *rmq.StateReleased:
			rmq.Warn("[Publisher]", "Message was not routed", publishResult.Message.Data[0])
		case *rmq.StateRejected:
			rmq.Warn("[Publisher]", "Message rejected", publishResult.Message.Data[0])
			stateType := publishResult.Outcome.(*rmq.StateRejected)
			if stateType.Error != nil {
				rmq.Warn("[Publisher]", "Message rejected with error: %v", stateType.Error)
			}
		default:
			// these status are not supported. Leave it for AMQP 1.0 compatibility
			// see: https://www.rabbitmq.com/docs/next/amqp#outcomes
			rmq.Warn("Message state: %v", publishResult.Outcome)
		}
	}

	println("press any key to close the connection")

	var input string
	_, _ = fmt.Scanln(&input)

	cancel()
	//Close the consumer
	err = consumer.Close(context.Background())
	if err != nil {
		rmq.Error("[Consumer]", err)
		return
	}
	// Close the publisher
	err = publisher.Close(context.Background())
	if err != nil {
		rmq.Error("[Publisher]", err)
		return
	}

	// Unbind the queue from the exchange
	err = management.Unbind(context.TODO(), bindingPath)

	if err != nil {
		rmq.Error("Error unbinding", "error", err)
		return
	}

	err = management.DeleteExchange(context.TODO(), exchangeInfo.Name())
	if err != nil {
		rmq.Error("Error deleting exchange", "error", err)
		return
	}

	// Purge the queue
	purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error purging queue", "error", err)
		return
	}
	rmq.Info("Purged messages from the queue", "count", purged)

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error deleting queue", "error", err)
		return
	}

	// Close all the connections. but you can still use the environment
	// to create new connections
	err = env.CloseConnections(context.Background())
	if err != nil {
		rmq.Error("Error closing connection", "error", err)
		return
	}

	rmq.Info("AMQP connection closed")
	// not necessary. It waits for the status change to be printed
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
