// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
// The example is demonstrating how to implement a simple RPC echo server and client using RabbitMQ AMQP 1.0 Go Client.
// It uses DirectReplyTo for the client to receive responses without needing to declare a reply queue.
// DirectReplyTo is the recommended way to receive replies for RPC clients.
// The server listens for messages on a request queue and responds with the same message (echo).
// The client sends messages to the request queue and waits for the echoed response.
// The example also includes graceful shutdown handling when the user presses Ctrl+C.
// Example path:https://github.com/rabbitmq/rabbitmq-amqp-go-client/tree/main/docs/examples/rpc_echo_server/main.go

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
	_ = srvConn.Management().DeleteQueue(context.TODO(), requestQueue)

	srv := newEchoResponder(srvConn)
	reply, _ := srv.server.GetRequestQueue()

	fmt.Printf("request queue %s \n", reply)
	// Dial rabbit for RPC client connection
	clientConn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", nil)
	if err != nil {
		panic(err)
	}

	requester, err := clientConn.NewRequester(context.TODO(), &rabbitmqamqp.RequesterOptions{
		RequestQueueName: requestQueue,
		// Use DirectReplyTo so replies are received via RabbitMQ direct-reply-to (no reply queue declared).
		// See: https://www.rabbitmq.com/docs/direct-reply-to#overview
		// That's the recommended way to receive replies for RPC clients,
		// as it avoids the overhead of declaring and consuming from a reply queue.
		SettleStrategy: rabbitmqamqp.DirectReplyTo,
	})
	if err != nil {
		panic(err)
	}
	reply, err = requester.GetReplyQueue()
	if err != nil {
		panic(fmt.Errorf("failed to get reply queue: %w", err))
	}
	fmt.Printf("replyTo to %s \n", reply)

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
