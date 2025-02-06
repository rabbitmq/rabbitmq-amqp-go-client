package main

import (
	"context"
	"errors"
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
		RecoveryConfiguration: &rabbitmq_amqp.RecoveryConfiguration{
			ActiveRecovery:           true,
			BackOffReconnectInterval: 2 * time.Second, // we reduce the reconnect interval to speed up the test. The default is 5 seconds
			// In production, you should avoid BackOffReconnectInterval with low values since it can cause a high number of reconnection attempts
			MaxReconnectAttempts: 5,
		},
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

	consumer, err := amqpConnection.NewConsumer(context.Background(), &rabbitmq_amqp.QueueAddress{
		Queue: queueName,
	}, "reliable-consumer")
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
				return
			}
			if err != nil {
				// An error occurred receiving the message
				rabbitmq_amqp.Error("[NewConsumer]", "Error receiving message, retry in 2_500 ms", err, "queue", queueName)
				// here the consumer could be disconnected from the server due to a network error
				// in this specific case, we just wait for 2_500 ms and try again (2 seconds is the reconnect interval we defined + random 500 random ms)
				// while the connection is reestablished
				// you can use the stateChanged channel to be notified when the connection is reestablished
				time.Sleep(2500 * time.Millisecond)
				continue
			}

			atomic.AddInt32(&received, 1)
			err = deliveryContext.Accept(context.Background())
			if err != nil {
				// same here the delivery could not be accepted due to a network error
				// we wait for 2_500 ms and try again
				time.Sleep(2500 * time.Millisecond)
				continue
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rabbitmq_amqp.QueueAddress{
		Queue: queueName,
	}, "reliable-publisher")
	if err != nil {
		rabbitmq_amqp.Error("Error creating publisher", err)
		return
	}

	for i := 0; i < 1_000_000; i++ {
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!"+fmt.Sprintf("%d", i))))
		if err != nil {
			rabbitmq_amqp.Error("Error publishing message", "error", err)
			// here you need to deal with the error. You can store the message in a local in memory/persistent storage
			// then retry to send the message as soon as the connection is reestablished
			// in this specific case, we just wait for 2_500 ms and try again (2 seconds is the reconnect interval we defined + random 500 random ms)
			// you can use the stateChanged channel to be notified when the connection is reestablished
			// and use some signal to reactivate the message sending
			atomic.AddInt32(&failed, 1)
			time.Sleep(2500 * time.Millisecond)
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
