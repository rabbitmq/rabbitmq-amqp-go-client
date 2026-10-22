// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ Streams documentation: https://www.rabbitmq.com/docs/streams
// The example is demonstrating how to declare a stream with an initial offset (RabbitMQ 4.4+).
// The first message published to the stream gets the configured initial offset instead of 0,
// the following messages get consecutive offsets. The consumer reads the offset from the
// "x-stream-offset" message annotation.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/stream_initial_offset/stream_initial_offset.go

package main

import (
	"context"
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

func main() {
	rmq.Info("Golang AMQP 1.0 Stream initial offset example")
	queueStream := "stream-initial-offset-" + time.Now().String()
	const initialOffset = 1000
	const messageCount = 5

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)
	amqpConnection, err := env.NewConnection(context.TODO())
	checkError(err)

	// declare a stream whose first message will have offset 1000.
	// This requires RabbitMQ 4.4 or later.
	_, err = amqpConnection.Management().DeclareQueue(context.TODO(), &rmq.StreamQueueSpecification{
		Name:           queueStream,
		MaxLengthBytes: rmq.CapacityGB(5),
		InitialOffset:  &rmq.InitialOffset{Value: initialOffset},
	})
	checkError(err)

	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueStream}, nil)
	checkError(err)

	for i := 0; i < messageCount; i++ {
		publishResult, err := publisher.Publish(context.TODO(), rmq.NewMessage([]byte("Hello World")))
		checkError(err)
		if _, ok := publishResult.Outcome.(*rmq.StateAccepted); !ok {
			rmq.Warn("[Publisher]", "Message not accepted", publishResult.Outcome)
		}
	}

	consumer, err := amqpConnection.NewConsumer(context.TODO(), queueStream, &rmq.StreamConsumerOptions{
		Offset: &rmq.OffsetFirst{},
	})
	checkError(err)

	for i := 0; i < messageCount; i++ {
		deliveryContext, err := consumer.Receive(context.TODO())
		checkError(err)
		// the offsets start from the initial offset: 1000, 1001, 1002, ...
		rmq.Info("[Consumer]", "Message received", string(deliveryContext.Message().Data[0]),
			"offset", deliveryContext.Message().Annotations["x-stream-offset"])
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
