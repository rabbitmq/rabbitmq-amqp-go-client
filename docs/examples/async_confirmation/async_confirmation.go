package main

import (
	"context"
	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"sync/atomic"
	"time"
)

func main() {

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	// Open a connection to the AMQP 1.0 server ( RabbitMQ >= 4.0)
	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}

	amqpConnection.Management().DeleteQueue(context.Background(), "test")
	amqpConnection.Management().DeclareQueue(context.Background(), &rmq.QuorumQueueSpecification{
		Name: "test",
	})

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rmq.QueueAddress{Queue: "test"}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}
	// start date

	startDate := time.Now()
	var confirmed int32
	// publish messages to the stream
	f := func(message *amqp.Message, state rmq.DeliveryState, err error) {

		switch state.(type) {
		case *amqp.StateAccepted:
			if atomic.AddInt32(&confirmed, 1)%20_000 == 0 {
				// confirmations per second
				rmq.Info("Confirmations per second", "value", float64(confirmed)/time.Since(startDate).Seconds())
			}
		default:
			panic("Message not accepted")

		}

	}

	//for i := 0; i < 500_000; i++ {
	//	_, err := publisher.Publish(context.Background(), rmq.NewMessage(make([]byte, 1)))
	//	if err != nil {
	//		rmq.Error("Error publishing message", err)
	//	}
	//	if i%20_000 == 0 {
	//		// message per second
	//		rmq.Info("Sync Messages per second", "value", float64(i)/time.Since(startDate).Seconds())
	//	}
	//}

	for i := 0; i < 500_000; i++ {
		err := publisher.PublishAsyncConfirmation(context.Background(), rmq.NewMessage(make([]byte, 1)), f)
		if err != nil {
			rmq.Error("Error publishing message", err)
		}
		if i%20_000 == 0 {
			// message per second
			rmq.Info("Messages per second", "value", float64(i)/time.Since(startDate).Seconds())
		}
	}

	time.Sleep(20 * time.Second)

}
