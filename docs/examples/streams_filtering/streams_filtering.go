package main

import (
	"context"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"time"
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

	rmq.Info("Golang AMQP 1.0 Streams example with filtering")
	queueStream := "stream-go-queue-filtering-" + time.Now().String()
	env := rmq.NewEnvironment([]string{"amqp://"}, nil)
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
	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rmq.QueueAddress{Queue: queueStream}, "stream-publisher")
	checkError(err)

	filters := []string{"MyFilter1", "MyFilter2", "MyFilter3", "MyFilter4"}

	// publish messages to the stream
	for i := 0; i < 40; i++ {
		msg := rmq.NewMessageWithFilter([]byte("Hello World"), filters[i%4])
		publishResult, err := publisher.Publish(context.Background(), msg)
		checkError(err)

		// check the outcome of the publishResult
		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher]", "Message accepted", publishResult.Message.Data[0])
			break
		case *rmq.StateReleased:
			rmq.Warn("[Publisher]", "Message was not routed", publishResult.Message.Data[0])
			break
		case *rmq.StateRejected:
			rmq.Warn("[Publisher]", "Message rejected", publishResult.Message.Data[0])
			stateType := publishResult.Outcome.(*rmq.StateRejected)
			if stateType.Error != nil {
				rmq.Warn("[Publisher]", "Message rejected with error: %v", stateType.Error)
			}
			break
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

		// add a filter to the consumer, in this case we use only the filter values
		// MyFilter1 and MyFilter2. So all other messages won't be received
		Filters: []string{"MyFilter1", "MyFilter2"},
	})
	checkError(err)

	// receive messages from the stream.
	// In this case we should receive only 20 messages with the filter values
	// MyFilter1 and MyFilter2
	for i := 0; i < 20; i++ {
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
