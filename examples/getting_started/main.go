package main

import (
	"bufio"
	"context"
	"fmt"
	mq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
	"os"
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
		return
	}

	fmt.Printf("AMQP Connection opened.\n")
	management := amqpConnection.Management()
	queueSpec := management.Queue("getting_started_queue").
		QueueType(mq.QueueType{Type: mq.Quorum}).
		MaxLengthBytes(mq.CapacityGB(1))
	exchangeSpec := management.Exchange("getting_started_exchange").
		ExchangeType(mq.ExchangeType{Type: mq.Topic})

	queueInfo, err := queueSpec.Declare(context.Background())
	if err != nil {
		fmt.Printf("Error declaring queue %s\n", err)
		return
	}
	fmt.Printf("Queue %s created.\n", queueInfo.GetName())

	exchangeInfo, err := exchangeSpec.Declare(context.Background())
	if err != nil {
		fmt.Printf("Error declaring exchange %s\n", err)
		return
	}
	fmt.Printf("Exchange %s created.\n", exchangeInfo.GetName())

	bindingSpec := management.Binding().SourceExchange(exchangeSpec).DestinationQueue(queueSpec).Key("routing-key")

	err = bindingSpec.Bind(context.Background())
	if err != nil {
		fmt.Printf("Error binding %s\n", err)
		return
	}

	fmt.Printf("Binding between %s and %s created.\n", exchangeInfo.GetName(), queueInfo.GetName())

	fmt.Println("Press any key to cleanup and exit")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')

	err = bindingSpec.Unbind(context.Background())
	if err != nil {
		fmt.Printf("Error unbinding %s\n", err)
		return
	}

	fmt.Printf("Binding between %s and %s deleted.\n", exchangeInfo.GetName(), queueInfo.GetName())

	err = exchangeSpec.Delete(context.Background())
	if err != nil {
		fmt.Printf("Error deleting exchange %s\n", err)
		return
	}

	err = queueSpec.Delete(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("Queue %s deleted.\n", queueInfo.GetName())

	err = amqpConnection.Close(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("AMQP Connection closed.\n")
	// Wait for the status change to be printed
	time.Sleep(500 * time.Millisecond)
	close(chStatusChanged)
}
