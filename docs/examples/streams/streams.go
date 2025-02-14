package main

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmq_amqp"
	"time"
)

func checkError(err error) {
	if err != nil {
		rabbitmq_amqp.Error("Error", err)
		// it should not happen for the example
		// so panic just to make sure we catch it
		panic(err)
	}
}

func main() {

	rabbitmq_amqp.Info("Golang AMQP 1.0 Streams example")
	queueStream := "stream-go-queue-" + time.Now().String()
	env := rabbitmq_amqp.NewEnvironment([]string{"amqp://"}, nil)
	amqpConnection, err := env.NewConnection(context.Background())
	checkError(err)
	management := amqpConnection.Management()
	// define a stream queue
	_, err = management.DeclareQueue(context.TODO(), &rabbitmq_amqp.StreamQueueSpecification{
		Name: queueStream,
		// it is a best practice to set the max length of the stream
		// to avoid the stream to grow indefinitely
		// the value here is low just for the example
		MaxLengthBytes: rabbitmq_amqp.CapacityGB(5),
	})
	checkError(err)

	// create a stream publisher. In this case we use the QueueAddress to make the example
	// simple. So we use the default exchange here.
	publisher, err := amqpConnection.NewPublisher(context.TODO(), &rabbitmq_amqp.QueueAddress{Queue: queueStream}, "stream-publisher")
	checkError(err)

	// publish messages to the stream
	for i := 0; i < 10; i++ {
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello World")))
		checkError(err)

		// check the outcome of the publishResult
		switch publishResult.Outcome.(type) {
		case *amqp.StateAccepted:
			rabbitmq_amqp.Info("[Publisher]", "Message accepted", publishResult.Message.Data[0])
			break
		case *amqp.StateReleased:
			rabbitmq_amqp.Warn("[Publisher]", "Message was not routed", publishResult.Message.Data[0])
			break
		case *amqp.StateRejected:
			rabbitmq_amqp.Warn("[Publisher]", "Message rejected", publishResult.Message.Data[0])
			stateType := publishResult.Outcome.(*amqp.StateRejected)
			if stateType.Error != nil {
				rabbitmq_amqp.Warn("[Publisher]", "Message rejected with error: %v", stateType.Error)
			}
			break
		default:
			// these status are not supported. Leave it for AMQP 1.0 compatibility
			// see: https://www.rabbitmq.com/docs/next/amqp#outcomes
			rabbitmq_amqp.Warn("Message state: %v", publishResult.Outcome)
		}

	}
	// create a stream consumer
	consumer, err := amqpConnection.NewConsumer(context.Background(), queueStream, &rabbitmq_amqp.StreamConsumerOptions{
		// the offset is set to the first chunk of the stream
		// so here it starts from the beginning
		Offset: &rabbitmq_amqp.OffsetFirst{},
	})
	checkError(err)

	// receive messages from the stream
	for i := 0; i < 10; i++ {
		deliveryContext, err := consumer.Receive(context.Background())
		checkError(err)
		rabbitmq_amqp.Info("[Consumer]", "Message received", deliveryContext.Message().Data[0])
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

	rabbitmq_amqp.Info("Example completed")
}
