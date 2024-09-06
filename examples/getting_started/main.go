package main

import (
	"context"
	"fmt"
	mq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
)

func main() {
	amqpConnection := mq.NewAmqpConnection()
	err := amqpConnection.Open(context.Background(), mq.NewConnectionSettings())
	if err != nil {
		return
	}

	management := amqpConnection.Management()
	queueSpec := management.Queue("getting_started_queue").
		QueueType(mq.QueueType{Type: mq.Quorum})
	queueInfo, err := queueSpec.Declare(context.Background())
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
