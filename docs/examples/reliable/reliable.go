package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmq_amqp"
	"sync/atomic"
	"time"
)

func main() {
	queueName := "reliable-amqp10-go-queue"
	var stateAccepted int32
	var stateReleased int32
	var stateRejected int32
	var received int32
	var failed int32

	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Printf("Messages sent: %d, accepted: %d, released: %d, rejected: %d, received: %d, failed: %d\n",
				stateAccepted+stateReleased+stateRejected, stateAccepted, stateReleased, stateRejected, received, failed)

		}
	}()

	rabbitmq_amqp.Info("How to deal with network disconnections")

	/// Create a channel to receive state change notifications
	stateChanged := make(chan *rabbitmq_amqp.StateChanged, 1)
	go func(ch chan *rabbitmq_amqp.StateChanged) {
		for statusChanged := range ch {
			rabbitmq_amqp.Info("[connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	// Open a connection to the AMQP 1.0 server
	amqpConnection, err := rabbitmq_amqp.Dial(context.Background(), []string{"amqp://"}, &rabbitmq_amqp.AmqpConnOptions{
		SASLType:    amqp.SASLTypeAnonymous(),
		ContainerID: "reliable-amqp10-go",
	})
	if err != nil {
		rabbitmq_amqp.Error("Error opening connection", err)
		return
	}
	// Register the channel to receive status change notifications
	amqpConnection.NotifyStatusChange(stateChanged)

	fmt.Printf("AMQP connection opened.\n")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rabbitmq_amqp.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rabbitmq_amqp.Error("Error declaring queue", err)
		return
	}

	//consumer, err := amqpConnection.NewConsumer(context.Background(), &rabbitmq_amqp.QueueAddress{
	//	Queue: queueName,
	//}, "reliable-consumer")
	//if err != nil {
	//	rabbitmq_amqp.Error("Error creating consumer", err)
	//	return
	//}

	//consumerContext, cancel := context.WithCancel(context.Background())

	//// Consume messages from the queue
	//go func(ctx context.Context) {
	//	for {
	//		deliveryContext, err := consumer.Receive(ctx)
	//		if errors.Is(err, context.Canceled) {
	//			// The consumer was closed correctly
	//			return
	//		}
	//		if err != nil {
	//			// An error occurred receiving the message
	//			rabbitmq_amqp.Error("[Consumer]", "Error receiving message", err)
	//			return
	//		}
	//
	//		rabbitmq_amqp.Info("[Consumer]", "Received message",
	//			fmt.Sprintf("%s", deliveryContext.Message().Data))
	//
	//		err = deliveryContext.Accept(context.Background())
	//		if err != nil {
	//			rabbitmq_amqp.Error("Error accepting message", err)
	//			return
	//		}
	//	}
	//}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rabbitmq_amqp.QueueAddress{
		Queue: queueName,
	}, "reliable-publisher")
	if err != nil {
		rabbitmq_amqp.Error("Error creating publisher", err)
		return
	}

	for i := 0; i < 1_000_000; i++ {
		// Publish a message to the exchange
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!"+fmt.Sprintf("%d", i))))
		if err != nil {
			rabbitmq_amqp.Error("Error publishing message", "error", err)
			atomic.AddInt32(&failed, 1)
			time.Sleep(1 * time.Second)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *amqp.StateAccepted:
			atomic.AddInt32(&stateAccepted, 1)
			break
		case *amqp.StateReleased:
			atomic.AddInt32(&stateReleased, 1)
			break
		case *amqp.StateRejected:
			atomic.AddInt32(&stateRejected, 1)
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

	//cancel()
	//Close the consumer
	//err = consumer.Close(context.Background())
	//if err != nil {
	//	rabbitmq_amqp.Error("[Consumer]", err)
	//	return
	//}
	// Close the publisher
	err = publisher.Close(context.Background())
	if err != nil {
		rabbitmq_amqp.Error("[Publisher]", err)
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

	fmt.Printf("AMQP connection closed.\n")
	// not necessary. It waits for the status change to be printed
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
