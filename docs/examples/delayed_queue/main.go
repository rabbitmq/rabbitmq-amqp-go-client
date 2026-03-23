// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// This example mirrors getting_started but declares a delayed queue via DelayedQueueSpecification
// (RabbitMQ queue type "delayed"). Delayed queues are available on Tanzu RabbitMQ 4.x+; see:
// https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-rabbitmq-on-kubernetes/4-2/tanzu-rabbitmq-kubernetes/delayed-queues.html
// example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/delayed_queue/main.go

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func main() {
	exchangeName := "delayed-queue-go-exchange"
	queueName := "delayed-queue-go-queue"
	routingKey := "routing-key"

	rmq.Info("Delayed queue example with AMQP Go AMQP 1.0 Client")

	stateChanged := make(chan *rmq.StateChanged, 1)
	go func(ch chan *rmq.StateChanged) {
		for statusChanged := range ch {
			rmq.Info("[connection]", "Status changed", statusChanged)
		}
	}(stateChanged)

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil)

	amqpConnection, err := env.NewConnection(context.Background())
	if err != nil {
		rmq.Error("Error opening connection", err)
		return
	}
	amqpConnection.NotifyStatusChange(stateChanged)

	rmq.Info("AMQP connection opened")
	management := amqpConnection.Management()
	exchangeInfo, err := management.DeclareExchange(context.TODO(), &rmq.TopicExchangeSpecification{
		Name: exchangeName,
	})
	if err != nil {
		rmq.Error("Error declaring exchange", err)
		return
	}

	// Declare a delayed queue (x-queue-type=delayed). Optional shovel and other arguments
	// can be set on DelayedQueueSpecification (see pkg/rabbitmqamqp/entities.go).
	queueInfo, err := management.DeclareQueue(context.TODO(), &rmq.DelayedQueueSpecification{
		Name: queueName,
	})

	if err != nil {
		rmq.Error("Error declaring queue", err)
		return
	}

	bindingPath, err := management.Bind(context.TODO(), &rmq.ExchangeToQueueBindingSpecification{
		SourceExchange:   exchangeName,
		DestinationQueue: queueName,
		BindingKey:       routingKey,
	})

	if err != nil {
		rmq.Error("Error binding", err)
		return
	}

	consumer, err := amqpConnection.NewConsumer(context.Background(), queueName, nil)
	if err != nil {
		rmq.Error("Error creating consumer", err)
		return
	}

	consumerContext, cancel := context.WithCancel(context.Background())

	go func(ctx context.Context) {
		for {
			deliveryContext, err := consumer.Receive(ctx)
			if errors.Is(err, context.Canceled) {
				rmq.Info("[Consumer] Consumer closed", "context", err)
				return
			}
			if err != nil {
				rmq.Error("[Consumer] Error receiving message", "error", err)
				return
			}

			rmq.Info("[Consumer] Received message", "message",
				fmt.Sprintf("%s", deliveryContext.Message().Data))

			err = deliveryContext.Accept(context.Background())
			if err != nil {
				rmq.Error("[Consumer] Error accepting message", "error", err)
				return
			}
		}
	}(consumerContext)

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rmq.ExchangeAddress{
		Exchange: exchangeName,
		Key:      routingKey,
	}, nil)
	if err != nil {
		rmq.Error("Error creating publisher", err)
		return
	}

	// Schedule delivery a few seconds in the future using message annotations (see Tanzu delayed queue docs).
	const delayMs int64 = 3000
	for i := 0; i < 5; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("Hello after delay #%d", i)))
		if msg.Annotations == nil {
			msg.Annotations = amqp.Annotations{}
		}
		msg.Annotations["x-opt-delivery-delay"] = delayMs

		publishResult, err := publisher.Publish(context.Background(), msg)
		if err != nil {
			rmq.Error("Error publishing message", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}
		switch publishResult.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Publisher]", "Message accepted (scheduled)", publishResult.Message.Data[0])
		case *rmq.StateReleased:
			rmq.Warn("[Publisher]", "Message was not routed", publishResult.Message.Data[0])
		case *rmq.StateRejected:
			rmq.Warn("[Publisher]", "Message rejected", publishResult.Message.Data[0])
			stateType := publishResult.Outcome.(*rmq.StateRejected)
			if stateType.Error != nil {
				rmq.Warn("[Publisher]", "Message rejected with error: %v", stateType.Error)
			}
		default:
			rmq.Warn("Message state: %v", publishResult.Outcome)
		}
	}

	println("press any key to close the connection")

	var input string
	_, _ = fmt.Scanln(&input)

	cancel()
	err = consumer.Close(context.Background())
	if err != nil {
		rmq.Error("[Consumer]", err)
		return
	}
	err = publisher.Close(context.Background())
	if err != nil {
		rmq.Error("[Publisher]", err)
		return
	}

	err = management.Unbind(context.TODO(), bindingPath)

	if err != nil {
		rmq.Error("Error unbinding", "error", err)
		return
	}

	err = management.DeleteExchange(context.TODO(), exchangeInfo.Name())
	if err != nil {
		rmq.Error("Error deleting exchange", "error", err)
		return
	}

	purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error purging queue", "error", err)
		return
	}
	rmq.Info("Purged messages from the queue", "count", purged)

	err = management.DeleteQueue(context.TODO(), queueInfo.Name())
	if err != nil {
		rmq.Error("Error deleting queue", "error", err)
		return
	}

	err = env.CloseConnections(context.Background())
	if err != nil {
		rmq.Error("Error closing connection", "error", err)
		return
	}

	rmq.Info("AMQP connection closed")
	time.Sleep(100 * time.Millisecond)
	close(stateChanged)
}
