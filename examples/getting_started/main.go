package main

import (
	"bufio"
	"context"
	"fmt"
	mq "github.com/rabbitmq/rabbitmq-amqp-go-client/rabbitmq_amqp"
	"os"
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
		MaxLengthBytes(mq.CapacityGB(1)).
		DeadLetterExchange("dead-letter-exchange").
		DeadLetterRoutingKey("dead-letter-routing-key")
	queueInfo, err := queueSpec.Declare(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("Queue %s created.\n", queueInfo.GetName())
	err = queueSpec.Delete(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("Queue %s deleted.\n", queueInfo.GetName())

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')

	err = amqpConnection.Close(context.Background())
	if err != nil {
		return
	}
	fmt.Printf("AMQP Connection closed.\n")
	close(chStatusChanged)
}
