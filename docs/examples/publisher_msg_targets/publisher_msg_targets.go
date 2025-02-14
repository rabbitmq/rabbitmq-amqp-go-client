package main

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmq_amqp"
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

	rabbitmq_amqp.Info("Define the publisher message targets")

	env := rabbitmq_amqp.NewEnvironment([]string{"amqp://"}, nil)
	amqpConnection, err := env.NewConnection(context.Background())
	checkError(err)
	queues := []string{"queue1", "queue2", "queue3"}
	management := amqpConnection.Management()
	for _, queue := range queues {
		_, err = management.DeclareQueue(context.TODO(), &rabbitmq_amqp.QuorumQueueSpecification{
			Name: queue,
		})
		checkError(err)
	}

	// create a publisher without a target
	publisher, err := amqpConnection.NewPublisher(context.TODO(), nil, "stream-publisher")
	checkError(err)

	// publish messages to the stream
	for i := 0; i < 12; i++ {

		// with this helper function we create a message with a target
		// that is the same to create a message with:
		//    msg := amqp.NewMessage([]byte("hello"))
		//    MessageToAddressHelper(msg, &QueueAddress{Queue: qName})
		//  same like:
		//     msg := amqp.NewMessage([]byte("hello"))
		//     msg.Properties = &amqp.MessageProperties{}
		//     msg.Properties.To = &address
		// NewMessageToAddress and MessageToAddressHelper helpers are provided to make the
		// code more readable and easier to use
		msg, err := rabbitmq_amqp.NewMessageToAddress([]byte("Hello World"),
			&rabbitmq_amqp.QueueAddress{Queue: queues[i%3]})
		checkError(err)
		publishResult, err := publisher.Publish(context.Background(), msg)
		checkError(err)
		switch publishResult.Outcome.(type) {
		case *amqp.StateAccepted:
			rabbitmq_amqp.Info("[Publisher]", "Message accepted", publishResult.Message.Data[0])
			break
		default:
			rabbitmq_amqp.Warn("[Publisher]", "Message not accepted", publishResult.Message.Data[0])
		}
	}

	// check the UI, you should see 4 messages in each queue

	// Close the publisher
	err = publisher.Close(context.Background())
	checkError(err)
	err = env.CloseConnections(context.Background())
	checkError(err)
}
