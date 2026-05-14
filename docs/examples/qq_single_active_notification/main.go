// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// Quorum queue + Single Active Consumer: FLOW link-state notifications (rabbitmq:active)
// via ConsumerOptions.SingleActiveConsumerStateChanged (RabbitMQ 4.3+).
//
//
//	Usage: go run . <producer|consumer>
//
// Example:
//
//	Terminal 1: go run . consumer
//	Terminal 2: go run . producer
//
// Run more than one consumer to see standby vs active notifications when the active consumer stops.
//
// Example path: https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/qq_single_active_notification/main.go

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	defaultQueueName = "demo-qq-sac-notification"
	defaultAMQPURL   = "amqp://guest:guest@localhost:5672/"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	mode := strings.ToLower(strings.TrimSpace(os.Args[1]))
	switch mode {
	case "producer":
		runProducer()
	case "consumer":
		runConsumer()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: go run . <producer|consumer>")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "  producer  — declare quorum queue (single-active-consumer) and publish messages")
	fmt.Fprintln(os.Stderr, "  consumer  — declare the same queue and consume; logs SAC active/standby state")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Requires RabbitMQ 4.3+ for SingleActiveConsumerStateChanged.")
}

func amqpURL() string {
	if u := strings.TrimSpace(os.Getenv("AMQP_URL")); u != "" {
		return u
	}
	return defaultAMQPURL
}

func declareSACQueue(ctx context.Context, management *rmq.AmqpManagement) error {
	_, err := management.DeclareQueue(ctx, &rmq.QuorumQueueSpecification{
		Name:                 defaultQueueName,
		SingleActiveConsumer: true,
	})
	return err
}

func runProducer() {
	ctx := context.TODO()
	conn, err := rmq.Dial(ctx, amqpURL(), nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	if err := declareSACQueue(ctx, conn.Management()); err != nil {
		fmt.Fprintln(os.Stderr, "declare queue:", err)
		os.Exit(1)
	}
	rmq.Info("Queue declared (quorum, single-active-consumer)", "queue", defaultQueueName)

	pub, err := conn.NewPublisher(ctx, &rmq.QueueAddress{Queue: defaultQueueName}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "publisher:", err)
		os.Exit(1)
	}
	defer pub.Close(ctx)

	const total = 3000
	for i := 0; i < total; i++ {
		time.Sleep(500 * time.Millisecond)
		body := []byte(fmt.Sprintf("SAC demo message #%d", i))
		res, err := pub.Publish(ctx, rmq.NewMessage(body))
		if err != nil {
			rmq.Error("publish failed", "error", err)
			continue
		}
		switch res.Outcome.(type) {
		case *rmq.StateAccepted:
			rmq.Info("[Producer] published and confirmed", "message", string(body))
		case *rmq.StateReleased:
			rmq.Info("[Producer] released (not routed)", "message", string(body))
		case *rmq.StateRejected:
			rmq.Error("[Producer] rejected", "message", string(body))
		default:
			rmq.Warn("[Producer] unexpected outcome", "outcome", fmt.Sprintf("%T", res.Outcome))
		}
	}
	rmq.Info("Producer finished publishing.")
}

func runConsumer() {
	ctx := context.TODO()
	conn, err := rmq.Dial(ctx, amqpURL(), nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	if err := declareSACQueue(ctx, conn.Management()); err != nil {
		fmt.Fprintln(os.Stderr, "declare queue:", err)
		os.Exit(1)
	}
	rmq.Info("Queue declared (quorum, single-active-consumer)", "queue", defaultQueueName)

	consumer, err := conn.NewConsumer(ctx, defaultQueueName, &rmq.ConsumerOptions{
		SettleStrategy: rmq.ExplicitSettle,
		SingleActiveConsumerStateChanged: func(_ *rmq.Consumer, isActive bool) {
			writeSacNotification(isActive)
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		os.Exit(1)
	}
	defer consumer.Close(ctx)

	recvCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for {
			dc, err := consumer.Receive(recvCtx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				rmq.Error("[Consumer] receive error", "error", err)
				return
			}
			msg := dc.Message()
			body := ""
			if len(msg.Data) > 0 {
				body = string(msg.Data[0])
			}
			rmq.Info("[Consumer] received", "message", body)
			if err := dc.Accept(ctx); err != nil {
				rmq.Error("[Consumer] accept error", "error", err)
				return
			}
		}
	}()

	fmt.Println("Consumer running. SingleActiveConsumerStateChanged fires when the broker marks this link active or standby (RabbitMQ 4.3+).")
	fmt.Println("Press Enter to exit.")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	cancel()
}

func writeSacNotification(isActive bool) {
	state := "STANDBY"
	if isActive {
		state = "ACTIVE (this link delivers messages)"
	}
	fmt.Println("********************************")
	fmt.Printf("[%s] Single Active Consumer state: %s\n", time.Now().Format("15:04:05.000"), state)
	fmt.Println("********************************")
}
