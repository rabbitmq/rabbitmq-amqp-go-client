// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// The example demonstrates how to use PublishAsync to send messages without blocking
// the caller while waiting for broker confirmation.
//
// Key concepts shown:
//   - PublishAsync fires the send immediately and delivers the outcome via a callback.
//   - MaxInFlight limits how many confirmation goroutines can run concurrently,
//     providing back-pressure: PublishAsync blocks the caller when the limit is reached.
//   - PublishTimeout controls how long each confirmation goroutine waits for the broker
//     before the callback receives a timeout error.
//   - A sync.WaitGroup is used to wait for all callbacks before shutting down.
//
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/publish_async/main.go

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	const (
		queueName      = "publish-async-go-queue"
		totalMessages  = 2_000_000
		maxInFlight    = 100
		publishTimeout = 10 * time.Second
	)

	rmq.Info("PublishAsync example starting")

	// Counters updated from the callback goroutines.
	var accepted, released, rejected, failed atomic.Int32

	// Track how long the whole publish loop takes.
	startTime := time.Now()

	// stateChanged receives connection lifecycle events.
	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for sc := range ch {
			rmq.Info("[connection] status changed", "from", sc.From, "to", sc.To)
		}
	}(stateChanged)

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", "error", err)
		return
	}
	amqpConnection.NotifyStatusChange(stateChanged)
	rmq.Info("AMQP connection opened")

	management := amqpConnection.Management()
	queueInfo, err := management.DeclareQueue(context.Background(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		rmq.Error("Error declaring queue", "error", err)
		return
	}

	// Create the publisher with async-specific options:
	//   MaxInFlight  – at most 100 confirmation goroutines run concurrently.
	//                  When the limit is reached, PublishAsync blocks the caller
	//                  until a slot becomes free.
	//   PublishTimeout – each confirmation goroutine waits at most 30 s for a
	//                    broker acknowledgement before the callback receives an error.
	publisher, err := amqpConnection.NewPublisher(context.Background(),
		&rmq.QueueAddress{Queue: queueName},
		&rmq.PublisherOptions{
			MaxInFlight:    maxInFlight,
			PublishTimeout: publishTimeout,
		})
	if err != nil {
		rmq.Error("Error creating publisher", "error", err)
		return
	}

	// wg is decremented once per callback invocation.
	var wg sync.WaitGroup
	wg.Add(totalMessages)

	for i := 0; i < totalMessages; i++ {
		msgBody := fmt.Sprintf("hello async %d", i)
		msg := rmq.NewMessage([]byte(msgBody))

		err = publisher.PublishAsync(context.Background(), msg,
			func(result *rmq.PublishResult, cbErr error) {
				defer wg.Done()

				if cbErr != nil {
					// Timeout or send-level error.
					rmq.Error("[Publisher] async callback error", "error", cbErr)
					failed.Add(1)
					return
				}

				switch result.Outcome.(type) {
				case *rmq.StateAccepted:
					accepted.Add(1)
				case *rmq.StateReleased:
					rmq.Warn("[Publisher] message not routed", "body", result.Message.Data[0])
					released.Add(1)
				case *rmq.StateRejected:
					s := result.Outcome.(*rmq.StateRejected)
					rmq.Warn("[Publisher] message rejected", "error", s.Error)
					rejected.Add(1)
				}
			})

		if err != nil {
			// PublishAsync only returns an error for validation failures or when the
			// caller's context is cancelled while waiting for a free in-flight slot.
			rmq.Error("[Publisher] PublishAsync error", "error", err)
			wg.Done()
		}
	}

	// Block until every callback has been invoked.
	rmq.Info("Waiting for all confirmations…")
	wg.Wait()

	elapsed := time.Since(startTime)
	rmq.Info("All messages confirmed",
		"accepted", accepted.Load(),
		"released", released.Load(),
		"rejected", rejected.Load(),
		"failed", failed.Load(),
		"elapsed", elapsed.Round(time.Millisecond),
		"msg/s", fmt.Sprintf("%.0f", float64(totalMessages)/elapsed.Seconds()),
	)

	if err = publisher.Close(context.Background()); err != nil {
		rmq.Error("Error closing publisher", "error", err)
	}

	// press any key to close
	println("press any key to close and clean up")

	var input string
	_, _ = fmt.Scanln(&input)

	purged, err := management.PurgeQueue(context.Background(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error purging queue", "error", err)
	} else {
		rmq.Info("Queue purged", "messages", purged)
	}

	if err = management.DeleteQueue(context.Background(), queueInfo.Name()); err != nil {
		rmq.Error("Error deleting queue", "error", err)
	}

	if err = env.CloseConnections(context.Background()); err != nil {
		rmq.Error("Error closing connection", "error", err)
	}

	rmq.Info("AMQP connection closed")
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
