package main

import (
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-amqp-dotnet-client/rabbitmq_amqp"
)

func main() {
	amqpConnection := rabbitmq_amqp.NewAmqpConnection()
	err := amqpConnection.Open(context.Background(), rabbitmq_amqp.NewConnectionSettings())
	if err != nil {
		return
	}

	management := amqpConnection.Management()
	queueSpec := management.Queue("getting_started_queue")
	err, queueInfo := queueSpec.Declare(context.Background())
	if err != nil {
		return
	}
	err = queueSpec.Delete(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("Queue name: %s\n", queueInfo.GetName())
	err = amqpConnection.Close(context.Background())
	if err != nil {
		return
	}

}
