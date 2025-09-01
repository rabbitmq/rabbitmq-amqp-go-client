package rabbitmqamqp_test

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	rpcServerQueueNameCustom = "rpc-queue-custom"
	correlationIDHeader      = "x-correlation-id"
)

type customCorrelationIDSupplier struct{}

func (s *customCorrelationIDSupplier) Get() any {
	return uuid.New().String()
}

func Example_customCorrelationId() {
	// Dial rabbit for RPC server connection
	srvConn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", nil)
	if err != nil {
		panic(err)
	}
	defer srvConn.Close(context.Background())

	_, err = srvConn.Management().DeclareQueue(context.TODO(), &rabbitmqamqp.QuorumQueueSpecification{
		Name: rpcServerQueueNameCustom,
	})
	if err != nil {
		panic(err)
	}

	server, err := srvConn.NewRpcServer(context.TODO(), rabbitmqamqp.RpcServerOptions{
		RequestQueue: rpcServerQueueNameCustom,
		Handler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
			fmt.Printf("Received: %s\n", request.GetData())
			return request, nil
		},
		CorrelationIdExtractor: func(message *amqp.Message) any {
			if message.ApplicationProperties == nil {
				panic("application properties are missing")
			}
			return message.ApplicationProperties[correlationIDHeader]
		},
		ReplyPostProcessor: func(reply *amqp.Message, correlationID any) *amqp.Message {
			if reply.ApplicationProperties == nil {
				reply.ApplicationProperties = make(map[string]interface{})
			}
			reply.ApplicationProperties[correlationIDHeader] = correlationID
			return reply
		},
	})
	if err != nil {
		panic(err)
	}
	defer server.Close(context.Background())

	// Dial rabbit for RPC client connection
	clientConn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", nil)
	if err != nil {
		panic(err)
	}
	defer clientConn.Close(context.Background())

	rpcClient, err := clientConn.NewRpcClient(context.TODO(), &rabbitmqamqp.RpcClientOptions{
		RequestQueueName:      rpcServerQueueNameCustom,
		CorrelationIdSupplier: &customCorrelationIDSupplier{},
		CorrelationIdExtractor: func(message *amqp.Message) any {
			if message.ApplicationProperties == nil {
				panic("application properties are missing")
			}
			return message.ApplicationProperties[correlationIDHeader]
		},
		RequestPostProcessor: func(request *amqp.Message, correlationID any) *amqp.Message {
			if request.ApplicationProperties == nil {
				request.ApplicationProperties = make(map[string]interface{})
			}
			request.ApplicationProperties[correlationIDHeader] = correlationID
			return request
		},
	})
	if err != nil {
		panic(err)
	}
	defer rpcClient.Close(context.Background())

	message := "hello world"
	resp, err := rpcClient.Publish(context.TODO(), amqp.NewMessage([]byte(message)))
	if err != nil {
		fmt.Printf("Error calling RPC: %v\n", err)
		return
	}

	m, ok := <-resp
	if !ok {
		fmt.Println("timed out waiting for response")
		return
	}
	fmt.Printf("Response: %s\n", m.GetData())
	// Output:
	// Received: hello world
	// Response: hello world
}
