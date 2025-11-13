package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type echoResponder struct {
	conn   *rabbitmqamqp.AmqpConnection
	server rabbitmqamqp.Responder
}

func (s *echoResponder) stop(ctx context.Context) {
	s.server.Close(ctx)
	s.conn.Close(ctx)
}

func newEchoResponder(conn *rabbitmqamqp.AmqpConnection) *echoResponder {
	_, err := conn.Management().DeclareQueue(context.TODO(), &rabbitmqamqp.QuorumQueueSpecification{
		Name: requestQueue,
	})
	if err != nil {
		panic(err)
	}
	srv, err := conn.NewResponder(context.TODO(), rabbitmqamqp.ResponderOptions{
		RequestQueue: requestQueue,
		Handler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
			return request, nil
		},
	})
	if err != nil {
		panic(err)
	}
	return &echoResponder{
		conn:   conn,
		server: srv,
	}
}

const requestQueue = "go-amqp1.0-request-queue"

func main() {
	// Dial rabbit for RPC server connection
	srvConn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", nil)
	if err != nil {
		panic(err)
	}

	srv := newEchoResponder(srvConn)

	// Dial rabbit for RPC client connection
	clientConn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", nil)
	if err != nil {
		panic(err)
	}

	requester, err := clientConn.NewRequester(context.TODO(), &rabbitmqamqp.RequesterOptions{
		RequestQueueName: requestQueue,
	})
	if err != nil {
		panic(err)
	}

	// Set up a channel to listen for OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt) // Listen for Ctrl+C

	// Goroutine to handle graceful shutdown
	go func() {
		<-sigs // Wait for Ctrl+C
		fmt.Println("\nReceived Ctrl+C, gracefully shutting down...")
		srv.stop(context.TODO())
		_ = clientConn.Close(context.TODO())
		_ = srvConn.Close(context.TODO())
		os.Exit(0)
	}()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Type a message and press Enter to send (Ctrl+C to quit):")

	for {
		fmt.Print("Enter message: ")
		input, _ := reader.ReadString('\n')
		// Remove newline character from input
		message := input[:len(input)-1]

		if message == "" {
			continue
		}

		resp, err := requester.Publish(context.TODO(), amqp.NewMessage([]byte(message)))
		if err != nil {
			fmt.Printf("Error calling RPC: %v\n", err)
			continue
		}
		m, ok := <-resp
		if !ok {
			fmt.Println("timed out waiting for response")
			continue
		}
		fmt.Printf("response: %s\n", m.GetData())
	}
}
