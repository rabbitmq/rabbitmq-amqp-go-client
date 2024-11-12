package main

import (
	"context"
	"fmt"
	mq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
	"time"
)

func main() {
	fmt.Printf("Getting started with AMQP Go AMQP 1.0 Client\n")
	chStatusChanged := make(chan *mq.StatusChanged, 1)

	go func(ch chan *mq.StatusChanged) {
		for statusChanged := range ch {
			fmt.Printf("Status changed from %d to %d\n", statusChanged.From, statusChanged.To)
		}
	}(chStatusChanged)

	amqpConnection := mq.NewAmqpConnection()
	amqpConnection.NotifyStatusChange(chStatusChanged)
	err := amqpConnection.Open(context.Background(), mq.NewConnectionSettings())
	if err != nil {
		fmt.Printf("Error opening connection: %v\n", err)
		return
	}
	fmt.Printf("AMQP Connection opened.\n")
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &mq.ExchangeSpecification{
		Name: "getting-started-exchange",
	})
	if err != nil {
		fmt.Printf("Error declaring exchange: %v\n", err)
		return
	}

	queueInfo, err := management.DeclareQueue(context.TODO(), &mq.QueueSpecification{
		Name:      "getting-started-queue",
		QueueType: mq.QueueType{Type: mq.Quorum},
	})

	if err != nil {
		fmt.Printf("Error declaring queue: %v\n", err)
		return
	}

	bindingPath, err := management.Bind(context.TODO(), &mq.BindingSpecification{
		SourceExchange:   exchangeInfo.Name(),
		DestinationQueue: queueInfo.Name(),
		BindingKey:       "routing-key",
	})

	err = management.Unbind(context.TODO(), bindingPath)

	if err != nil {
		fmt.Printf("Error unbinding: %v\n", err)
		return
	}

	err = management.DeleteExchange(context.TODO(), exchangeInfo.Name())
	if err != nil {
		fmt.Printf("Error deleting exchange: %v\n", err)
		return
	}

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		fmt.Printf("Error deleting queue: %v\n", err)
		return
	}

	err = management.Close(context.Background())
	if err != nil {
		fmt.Printf("Error closing management: %v\n", err)
		return
	}

	err = amqpConnection.Close(context.Background())
	if err != nil {
		fmt.Printf("Error closing connection: %v\n", err)
		return
	}

	fmt.Printf("AMQP Connection closed.\n")
	// Wait for the status change to be printed
	time.Sleep(500 * time.Millisecond)

	close(chStatusChanged)
}
