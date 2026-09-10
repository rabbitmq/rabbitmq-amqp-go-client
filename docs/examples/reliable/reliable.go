// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// The example is demonstrating how to implement a reliable message publishing and consuming scenario using RabbitMQ AMQP 1.0 Go Client.
// The example includes handling network disconnections, message acknowledgments, and tracking message states (accepted, released, rejected).
// It simulates a publisher sending messages to a queue and a consumer receiving those messages while dealing with potential network issues.
// The example also includes logging and statistics to monitor the message flow and connection status.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/reliable/reliable.go

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func main() {
	queueName := getEnv("QUEUE_NAME", "reliable-amqp10-go-queue")
	isSilent := getEnvBool("IS_SILENT", false)
	messagesToSend := getEnvInt("MESSAGES_TO_SEND", 500_000)
	delaymessage := getEnvBool("DELAY_MESSAGE", false)
	address := getEnv("ADDRESS", "amqp://guest:guest@localhost:5672/")
	var stateAccepted int32
	var stateReleased int32
	var stateRejected int32
	var isRunning bool

	var received int32
	var failed int32

	startTime := time.Now()
	isRunning = true
	go func() {
		for isRunning {
			time.Sleep(5 * time.Second)
			total := stateAccepted + stateReleased + stateRejected
			messagesPerSecond := float64(total) / time.Since(startTime).Seconds()
			rmq.Info("[go-amqp1.0 v:"+rmq.ClientVersion+"][Stats]", "sent", total, "received", received, "failed", failed, "messagesPerSecond", messagesPerSecond)
		}
	}()

	rmq.Info("[go-amqp1.0] How to deal with network disconnections")
	signalBlock := sync.Cond{L: &sync.Mutex{}}
	/// Create a channel to receive state change notifications
	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for statusChanged := range ch {
			rmq.Info("[go-amqp1.0] [connection]", "Status changed", statusChanged)
			switch statusChanged.To.(type) {
			case *rmq.StateOpen:
				signalBlock.Broadcast()
			case *rmq.StateReconnecting:
				rmq.Info("[go-amqp1.0] [connection]", "Reconnecting to the AMQP 1.0 server")
			case *rmq.StateClosed:
				StateClosed := statusChanged.To.(*rmq.StateClosed)
				if errors.Is(StateClosed.GetError(), rmq.ErrMaxReconnectAttemptsReached) {
					rmq.Error("[go-amqp1.0] [connection]", "Max reconnect attempts reached. Closing connection", StateClosed.GetError())
					signalBlock.Broadcast()
					isRunning = false
				}

			}
		}
	}(stateChanged)

	// Open a connection to the AMQP 1.0 server
	amqpConnection, err := rmq.Dial(context.TODO(), address, &rmq.AmqpConnOptions{
		SASLType:    amqp.SASLTypeAnonymous(),
		ContainerID: "reliable-amqp10-go",
		RecoveryConfiguration: &rmq.RecoveryConfiguration{
			ActiveRecovery:           true,
			BackOffReconnectInterval: 2 * time.Second, // we reduce the reconnect interval to speed up the test. The default is 5 seconds
			// In production, you should avoid BackOffReconnectInterval with low values since it can cause a high number of reconnection attempts
			MaxReconnectAttempts: 5,
		},
	})
	if err != nil {
		rmq.Error("[go-amqp1.0] Error opening connection", err)
		return
	}
	// Register the channel to receive status change notifications
	amqpConnection.NotifyStatusChange(stateChanged)

	rmq.Info("[go-amqp1.0] AMQP connection opened")
	// Create the management interface for the connection
	// so we can declare exchanges, queues, and bindings
	management := amqpConnection.Management()

	// Declare a Quorum queue
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("[go-amqp1.0] Error declaring queue", err)
		return
	}

	consumer, err := amqpConnection.NewConsumer(context.TODO(), queueName, nil)
	if err != nil {
		rmq.Error("[go-amqp1.0] Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.TODO())

	// Consume messages from the queue
	go func(ctx context.Context) {
		for isRunning {
			deliveryContext, err := consumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				// The consumer was closed correctly
				return
			}
			if err != nil && isRunning {
				// An error occurred receiving the message
				// here the consumer could be disconnected from the server due to a network error
				signalBlock.L.Lock()
				rmq.Info("[go-amqp1.0] [Consumer]", "Consumer is blocked, queue", queueName, "error", err)
				signalBlock.Wait()
				rmq.Info("[go-amqp1.0] [Consumer]", "Consumer is unblocked, queue", queueName)

				signalBlock.L.Unlock()
				continue
			}

			// validate the message and process it
			//var messageData = fmt.Sprintf("[go-amqp1.0] message id%d", i)
			//var message = rmq.NewMessage([]byte(messageData))
			//message.ApplicationProperties = make(map[string]interface{})
			//message.ApplicationProperties["message-id"] = i
			//message.ApplicationProperties["timestamp"] = time.Now().UnixMilli()
			//message.ApplicationProperties["from"] = "go-amqp1.0"
			if deliveryContext.Message().Data == nil {
				panic("[go-amqp1.0] [Consumer] Received message with nil data")
			}
			var body = fmt.Sprintf("%s", deliveryContext.Message().Data)
			if !strings.Contains(body, "message id") {
				panic("[go-amqp1.0] [Consumer] Received message with empty data")
			}

			if deliveryContext.Message().ApplicationProperties == nil {
				panic("[go-amqp1.0] [Consumer] Received message with nil ApplicationProperties")
			}

			if deliveryContext.Message().ApplicationProperties["message-id"] == nil {
				panic("[go-amqp1.0] [Consumer] Received message with nil message-id")
			}

			if deliveryContext.Message().ApplicationProperties["timestamp"] == nil {
				panic("[go-amqp1.0] [Consumer] Received message with nil timestamp")
			}

			if deliveryContext.Message().ApplicationProperties["from"] == nil {
				panic("[go-amqp1.0] [Consumer] Received message with nil from")
			}

			atomic.AddInt32(&received, 1)
			err = deliveryContext.Accept(context.TODO())
			if err != nil && isRunning {
				// same here the delivery could not be accepted due to a network error
				// we wait for 2_500 ms and try again
				time.Sleep(2500 * time.Millisecond)
				continue
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		rmq.Error("[go-amqp1.0] Error creating publisher", err)
		return
	}

	for i := 0; i < 1; i++ {
		go func() {
			for i := 0; i < messagesToSend; i++ {
				if !isRunning {
					rmq.Info("[go-amqp1.0] [Publisher]", "Publisher is stopped simulation not running, queue", queueName)
					return
				}
				var messageData = fmt.Sprintf("[go-amqp1.0] message id%d", i)
				var message = rmq.NewMessage([]byte(messageData))
				message.ApplicationProperties = make(map[string]interface{})
				message.ApplicationProperties["message-id"] = i
				message.ApplicationProperties["timestamp"] = time.Now().UnixMilli()
				message.ApplicationProperties["from"] = "go-amqp1.0"

				publishResult, err := publisher.Publish(context.TODO(), message)
				if err != nil {
					// here you need to deal with the error. You can store the message in a local in memory/persistent storage
					// then retry to send the message as soon as the connection is reestablished

					atomic.AddInt32(&failed, 1)
					// block signalBlock until the connection is reestablished
					signalBlock.L.Lock()
					rmq.Info("[go-amqp1.0] [Publisher]", "Publisher is blocked, queue", queueName, "error", err)
					signalBlock.Wait()
					rmq.Info("[go-amqp1.0] [Publisher]", "Publisher is unblocked, queue", queueName)
					signalBlock.L.Unlock()

				} else {
					switch publishResult.Outcome.(type) {
					case *rmq.StateAccepted:
						atomic.AddInt32(&stateAccepted, 1)
					case *rmq.StateReleased:
						atomic.AddInt32(&stateReleased, 1)
					case *rmq.StateRejected:
						atomic.AddInt32(&stateRejected, 1)
					default:
						// these status are not supported. Leave it for AMQP 1.0 compatibility
						// see: https://www.rabbitmq.com/docs/next/amqp#outcomes
						rmq.Warn("Message state: %v", publishResult.Outcome)
					}
				}
				if delaymessage {
					time.Sleep(200 * time.Millisecond)
				}
			}
		}()
	}

	if !isSilent {
		println("press any key to close the connection")

		var input string
		_, _ = fmt.Scanln(&input)

		cancel()
		//Close the consumer
		err = consumer.Close(context.TODO())
		if err != nil {
			rmq.Error("[NewConsumer]", err)
			return
		}
		// Close the publisher
		err = publisher.Close(context.TODO())
		if err != nil {
			rmq.Error("[NewPublisher]", err)
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

		err = amqpConnection.Close(context.TODO())
		if err != nil {
			fmt.Printf("Error closing connection: %v\n", err)
			return
		}

		fmt.Printf("AMQP connection closed.\n")
		// not necessary. It waits for the status change to be printed
		time.Sleep(100 * time.Millisecond)
		close(stateChanged)
	} else {
		/// loop forever
		for {
			time.Sleep(1 * time.Second)
		}
	}
}
