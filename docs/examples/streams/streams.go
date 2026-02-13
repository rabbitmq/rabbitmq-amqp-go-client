// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// RabbitMQ Streams documentation: https://www.rabbitmq.com/docs/streams
// The example is demonstrating how to use RabbitMQ AMQP 1.0 Go Client to work with RabbitMQ Streams.
// In this example, we declare a stream queue, publish messages to it, and then consume those messages from the stream.
// The example includes error handling and logging for each step of the process, and it demonstrates how to work with RabbitMQ Streams using the AMQP 1.0 protocol.
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/streams/streams.go

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

	rmq.Info("Golang AMQP 1.0 Streams example")
	queueStream := "stream-go-queue-" + time.Now().String()
	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	amqpConnection, err := env.NewConnection(context.Background())
	checkError(err)
	management := amqpConnection.Management()
	// define a stream queue
	_, err = management.DeclareQueue(context.TODO(), &rmq.StreamQueueSpecification{
		Name: queueStream,
		// it is a best practice to set the max length of the stream
		// to avoid the stream to grow indefinitely
		// the value here is low just for the example
		MaxLengthBytes: rmq.CapacityGB(5),
	})
	checkError(err)

	// create a stream publisher. In this case we use the QueueAddress to make the example
	// simple. So we use the default exchange here.
	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueStream}, nil)
	checkError(err)

	// publish messages to the stream
	for i := 0; i < 10; i++ {
		publishResult, err := publisher.Publish(context.Background(), rmq.NewMessage([]byte("Hello World")))
		checkError(err)

		// check the outcome of the publishResult
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
	// create a stream consumer
	consumer, err := amqpConnection.NewConsumer(context.Background(), queueStream, &rmq.StreamConsumerOptions{
		// the offset is set to the first chunk of the stream
		// so here it starts from the beginning
		Offset: &rmq.OffsetFirst{},
	})
	checkError(err)

	// receive messages from the stream
	for i := 0; i < 10; i++ {
		deliveryContext, err := consumer.Receive(context.Background())
		checkError(err)
		rmq.Info("[Consumer]", "Message received", deliveryContext.Message().Data[0])
		// accept the message
		err = deliveryContext.Accept(context.Background())
		checkError(err)
	}

	// close the consumer
	err = consumer.Close(context.Background())
	checkError(err)

	err = amqpConnection.Management().DeleteQueue(context.Background(), queueStream)
	checkError(err)

	err = env.CloseConnections(context.Background())
	checkError(err)

	rmq.Info("Example completed")
}
