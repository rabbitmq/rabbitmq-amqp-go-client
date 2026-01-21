// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// The example is demonstrating how to connect to RabbitMQ using AMQP 1.0 over WebSocket protocol with SASLTypePlain,
// declare a queue, publish a message to it, and then consume that message.
// AMQP 1.0 over WebSocket documentation: https://www.rabbitmq.com/blog/2025/04/16/amqp-websocket
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/web_sockets/web_sockets.go

package main

import (
	"context"

	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	const amqpConnectionString = "ws://127.0.0.1:15678/ws"
	rmq.Info("[Example]", "Starting web socket connection to", amqpConnectionString)
	// for anonymous connection use:
	// env := rmq.NewEnvironment(amqpConnectionString, &rmq.AmqpConnOptions{
	// 	SASLType: amqp.SASLTypeAnonymous(),
	// })
	env := rmq.NewEnvironment(amqpConnectionString, &rmq.AmqpConnOptions{
		SASLType: amqp.SASLTypePlain("rabbit", "rabbit"),
	})
	conn, err := env.NewConnection(context.Background())
	if err != nil {
		panic(err)
	}
	_, err = conn.Management().DeclareQueue(context.TODO(), &rmq.QuorumQueueSpecification{
		Name: "test-ws-queue",
	})
	if err != nil {
		panic(err)
	}
	// declare new producer
	producer, err := conn.NewPublisher(context.TODO(), &rmq.QueueAddress{
		Queue: "test-ws-queue",
	}, nil)
	if err != nil {
		panic(err)
	}
	msg := rmq.NewMessage([]byte("Hello over WebSockets"))

	publishResult, err := producer.Publish(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	switch publishResult.Outcome.(type) {
	case *rmq.StateAccepted:
		rmq.Info("[Publisher]", "Message accepted", publishResult.Message.Data[0])
	default:
		rmq.Warn("[Publisher]", "Message not accepted", publishResult.Message.Data[0])
	}

	// declare new consumer
	consumer, err := conn.NewConsumer(context.TODO(), "test-ws-queue", nil)
	if err != nil {
		panic(err)
	}
	deliveryContext, err := consumer.Receive(context.Background())
	if err != nil {
		panic(err)
	}
	rmq.Info("[Consumer]", "Message received", string(deliveryContext.Message().GetData()))
	err = deliveryContext.Accept(context.Background())
	if err != nil {
		panic(err)
	}
	// clean up
	err = consumer.Close(context.TODO())
	if err != nil {
		panic(err)
	}
	err = producer.Close(context.TODO())
	if err != nil {
		panic(err)
	}

	err = conn.Management().DeleteQueue(context.Background(), "test-ws-queue")
	if err != nil {
		panic(err)
	}

	err = conn.Close(context.TODO())
	if err != nil {
		panic(err)
	}

}
