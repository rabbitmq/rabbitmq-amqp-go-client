package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmq_amqp"
	"sync"
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

	startTime := time.Now()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			total := stateAccepted + stateReleased + stateRejected
			messagesPerSecond := float64(total) / time.Since(startTime).Seconds()
			rabbitmq_amqp.Info("[Stats]", "sent", total, "received", received, "failed", failed, "messagesPerSecond", messagesPerSecond)

		}
	}()

	rabbitmq_amqp.Info("How to deal with network disconnections")
	signalBlock := sync.Cond{L: &sync.Mutex{}}
	/// Create a channel to receive state change notifications
	stateChanged := make(chan *rabbitmq_amqp.StateChanged, 1)
	go func(ch chan *rabbitmq_amqp.StateChanged) {
		for statusChanged := range ch {
			rabbitmq_amqp.Info("[connection]", "Status changed", statusChanged)
			switch statusChanged.To.(type) {
			case *rabbitmq_amqp.StateOpen:
				signalBlock.Broadcast()
			}
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
				// here the consumer could be disconnected from the server due to a network error
				signalBlock.L.Lock()
				rabbitmq_amqp.Info("[Consumer]", "Consumer is blocked, queue", queueName, "error", err)
				signalBlock.Wait()
				rabbitmq_amqp.Info("[Consumer]", "Consumer is unblocked, queue", queueName)

				signalBlock.L.Unlock()
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

	wg := &sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500_000; i++ {
				publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!"+fmt.Sprintf("%d", i))))
				if err != nil {
					// here you need to deal with the error. You can store the message in a local in memory/persistent storage
					// then retry to send the message as soon as the connection is reestablished

					atomic.AddInt32(&failed, 1)
					// block signalBlock until the connection is reestablished
					signalBlock.L.Lock()
					rabbitmq_amqp.Info("[Publisher]", "Publisher is blocked, queue", queueName, "error", err)
					signalBlock.Wait()
					rabbitmq_amqp.Info("[Publisher]", "Publisher is unblocked, queue", queueName)
					signalBlock.L.Unlock()

				} else {
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
			}
		}()
	}
	wg.Wait()

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
