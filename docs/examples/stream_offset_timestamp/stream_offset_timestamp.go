// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// RabbitMQ Streams documentation: https://www.rabbitmq.com/docs/streams
// The example is demonstrating how to start consuming a stream from a point in time
// using the OffsetTimestamp offset specification.
// In this example, we publish a first batch of messages, wait a bit and take a timestamp,
// then publish a second batch. A consumer attached with OffsetTimestamp receives
// the messages published from that point in time and skips the first batch.
// Note: the stream stores messages in chunks, and the timestamp has millisecond precision.
// The broker starts from the first chunk with a timestamp greater than or equal to the given one,
// so messages published a few milliseconds before the timestamp may still be delivered.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/stream_offset_timestamp/stream_offset_timestamp.go

package main

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func checkError(err error) {
	if err != nil {
		rmq.Error("Error", err)
		// it should not happen for the example
		// so panic just to make sure we catch it
		panic(err)
	}
}

func publish(publisher *rmq.Publisher, label string, count int) {
	for i := 0; i < count; i++ {
		body := fmt.Sprintf("%s message #%d", label, i)
		publishResult, err := publisher.Publish(context.TODO(), rmq.NewMessage([]byte(body)))
		checkError(err)
		if _, ok := publishResult.Outcome.(*rmq.StateAccepted); !ok {
			panic(fmt.Sprintf("message not accepted: %v", publishResult.Outcome))
		}
	}
}

func main() {
	rmq.Info("Golang AMQP 1.0 Streams OffsetTimestamp example")
	queueStream := "stream-offset-timestamp-" + time.Now().String()
	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	amqpConnection, err := env.NewConnection(context.TODO())
	checkError(err)

	_, err = amqpConnection.Management().DeclareQueue(context.TODO(), &rmq.StreamQueueSpecification{
		Name:           queueStream,
		MaxLengthBytes: rmq.CapacityGB(5),
	})
	checkError(err)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueStream}, nil)
	checkError(err)

	// first batch: published BEFORE the timestamp
	publish(publisher, "before", 5)
	rmq.Info("[Publisher]", "Published", 5, "when", "before the timestamp")

	// the sleeps keep the timestamp away from the chunks of the two batches
	time.Sleep(2 * time.Second)
	startFrom := time.Now()
	rmq.Info("[Example]", "Timestamp", startFrom.Format(time.RFC3339Nano))
	time.Sleep(2 * time.Second)

	// second batch: published AFTER the timestamp
	publish(publisher, "after", 5)
	rmq.Info("[Publisher]", "Published", 5, "when", "after the timestamp")

	// start consuming from the timestamp: only the "after" messages are expected
	consumer, err := amqpConnection.NewConsumer(context.TODO(), queueStream, &rmq.StreamConsumerOptions{
		Offset: &rmq.OffsetTimestamp{Timestamp: startFrom},
	})
	checkError(err)

	for i := 0; i < 5; i++ {
		deliveryContext, err := consumer.Receive(context.TODO())
		checkError(err)
		rmq.Info("[Consumer]", "Message received", string(deliveryContext.Message().GetData()))
		err = deliveryContext.Accept(context.TODO())
		checkError(err)
	}

	err = consumer.Close(context.TODO())
	checkError(err)

	err = amqpConnection.Management().DeleteQueue(context.TODO(), queueStream)
	checkError(err)

	err = env.CloseConnections(context.TODO())
	checkError(err)

	rmq.Info("Example completed")
}
