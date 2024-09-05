package main

import (
	"context"
	"fmt"
	amqp "github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-dotnet-client/rabbitmq_amqp"
)

func main() {

	//nullGolangMessage := amqp.Message{}
	//nullGolangMessage := amqp.NewMessage(nil)
	nullGolangMessage := amqp.Message{}
	var null byte
	null = 64
	nullGolangMessage.Value = null

	binary, err1 := nullGolangMessage.MarshalBinary()
	if err1 != nil {
		return
	}
	fmt.Printf("Binary: %v\n", binary)
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
	//time.Sleep(3 * time.Second)

	err = queueSpec.Delete(context.Background())
	if err != nil {
		return
	}

	//time.Sleep(1 * time.Second)
	fmt.Printf("Queue name: %s\n", queueInfo.GetName())
	err = amqpConnection.Close(context.Background())
	if err != nil {
		return
	}

}
