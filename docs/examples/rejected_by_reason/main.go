// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example demonstrates the "rejected-by and rejection reason" feature introduced
// in RabbitMQ 4.3.
//
// When a queue rejects a message (e.g. because its maximum length has been reached),
// the broker now returns the queue name and a human-readable rejection reason inside
// the AMQP 1.0 Rejected outcome.  The Go client surfaces this through the
// PublishResult.MessageRejectedError field, which is non-nil only when the outcome
// is StateRejected and the broker supplied those details.
//
// Key concepts shown:
//   - Declare a quorum queue with MaxLength and RejectPublishOverflowStrategy so
//     that overflow messages are rejected rather than dropped.
//   - Inspect PublishResult.MessageRejectedError after a rejected publish to read
//     RejectedBy (the queue that rejected the message) and Reason (why it was rejected).
//   - The same information is available in the PublishAsync callback via the same field.
//
// Requires RabbitMQ 4.3 or later.
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#amqp-rejection-reason
//
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/rejected_by_reason/main.go

package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	const (
		queueName = "rejected-by-reason-go-queue"
		maxLength = 3
		totalSent = 10
	)

	rmq.Info("Rejected-by and rejection-reason example starting (requires RabbitMQ 4.3+)")

	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for sc := range ch {
			rmq.Info("[connection] status changed", "from", sc.From, "to", sc.To)
		}
	}(stateChanged)

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	amqpConnection, err := env.NewConnection(context.TODO())
	if err != nil {
		rmq.Error("Error opening connection", "error", err)
		return
	}
	amqpConnection.NotifyStatusChange(stateChanged)
	rmq.Info("AMQP connection opened")

	management := amqpConnection.Management()

	// Declare a quorum queue that rejects messages once it holds more than maxLength
	// messages.  The RejectPublishOverflowStrategy causes the broker to send back a
	// Rejected outcome (with rejection details on RabbitMQ 4.3+) instead of silently
	// dropping the overflowing message.
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name:             queueName,
		MaxLength:        maxLength,
		OverflowStrategy: &rmq.RejectPublishOverflowStrategy{},
	})
	if err != nil {
		rmq.Error("Error declaring queue", "error", err)
		return
	}
	rmq.Info("Queue declared", "name", queueInfo.Name(), "maxLength", maxLength)

	publisher, err := amqpConnection.NewPublisher(context.TODO(),
		&rmq.QueueAddress{Queue: queueName}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", "error", err)
		return
	}

	// ── Synchronous Publish ──────────────────────────────────────────────────────
	//
	// Publish totalSent messages.  The first maxLength will be accepted; every
	// subsequent message will be rejected because the queue is full.
	rmq.Info("--- Synchronous Publish ---")
	for i := 1; i <= totalSent; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("message %d", i)))
		result, pubErr := publisher.Publish(context.TODO(), msg)
		if pubErr != nil {
			rmq.Error("[Publisher] send error", "error", pubErr)
			continue
		}

		switch result.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher] accepted", "message", i)

		case *rmq.StateRejected:
			// MessageRejectedError is non-nil when RabbitMQ 4.3+ provides details.
			if result.MessageRejectedError != nil {
				rmq.Warn("[Publisher] rejected",
					"message", i,
					"rejected-by", result.MessageRejectedError.RejectedBy,
					"reason", result.MessageRejectedError.Reason,
				)
			} else {
				// Broker did not supply rejection details (pre-4.3 behaviour).
				rmq.Warn("[Publisher] rejected (no details)", "message", i)
			}

		case *rmq.StateReleased:
			rmq.Warn("[Publisher] released (unroutable)", "message", i)
		}
	}

	// Purge the queue so the async demo starts from an empty queue.
	purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error purging queue", "error", err)
		return
	}
	rmq.Info("Queue purged before async demo", "purged", purged)

	// ── Asynchronous PublishAsync ────────────────────────────────────────────────
	//
	// The same MessageRejectedError field is available inside the callback.
	rmq.Info("--- PublishAsync ---")
	var (
		wg       sync.WaitGroup
		accepted atomic.Int32
		rejected atomic.Int32
	)
	wg.Add(totalSent)

	for i := 1; i <= totalSent; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("async message %d", i)))
		msgIndex := i
		// add some delay give the time to store the message
		// and give the opportunity to max len feature to work as excepted
		// for this specific example
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		asyncErr := publisher.PublishAsync(context.TODO(), msg,
			func(result *rmq.PublishResult, cbErr error) {
				defer wg.Done()
				if cbErr != nil {
					rmq.Error("[Publisher] async callback error", "error", cbErr)
					return
				}

				switch result.Outcome.(type) {
				case *rmq.StateAccepted:
					accepted.Add(1)
					rmq.Info("[Publisher] async accepted", "message", msgIndex)

				case *rmq.StateRejected:
					rejected.Add(1)
					if result.MessageRejectedError != nil {
						rmq.Warn("[Publisher] async rejected",
							"message", msgIndex,
							"rejected-by", result.MessageRejectedError.RejectedBy,
							"reason", result.MessageRejectedError.Reason,
						)
					} else {
						rmq.Warn("[Publisher] async rejected (no details)", "message", msgIndex)
					}

				case *rmq.StateReleased:
					rmq.Warn("[Publisher] async released (unroutable)", "message", msgIndex)
				}
			})

		if asyncErr != nil {
			rmq.Error("[Publisher] PublishAsync error", "error", asyncErr)
			wg.Done()
		}
	}

	wg.Wait()
	rmq.Info("Async publish complete", "accepted", accepted.Load(), "rejected", rejected.Load())

	// ── Clean up ─────────────────────────────────────────────────────────────────
	println("\npress any key to clean up and exit")
	var input string
	_, _ = fmt.Scanln(&input)

	if err = publisher.Close(context.TODO()); err != nil {
		rmq.Error("Error closing publisher", "error", err)
	}

	if _, err = management.PurgeQueue(context.TODO(), queueInfo.Name()); err != nil {
		rmq.Error("Error purging queue", "error", err)
	}

	if err = management.DeleteQueue(context.TODO(), queueInfo.Name()); err != nil {
		rmq.Error("Error deleting queue", "error", err)
	}

	if err = env.CloseConnections(context.TODO()); err != nil {
		rmq.Error("Error closing connection", "error", err)
	}

	rmq.Info("AMQP connection closed")
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
