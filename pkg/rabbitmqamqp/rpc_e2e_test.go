package rabbitmqamqp_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// fibonacci calculates and returns the Fibonacci number of x.
// For x <= 1, it returns x. For x > 1, it calculates iteratively to avoid
// stack overflow for large values.
func fibonacci(x int) int {
	if x <= 1 {
		return x
	}

	a, b := 0, 1
	for i := 2; i <= x; i++ {
		a, b = b, a+b
	}
	return b
}

var _ = Describe("RPC E2E", Label("e2e"), func() {
	var (
		clientConn   *rabbitmqamqp.AmqpConnection
		serverConn   *rabbitmqamqp.AmqpConnection
		rpcClient    rabbitmqamqp.RpcClient
		rpcServer    rabbitmqamqp.RpcServer
		rpcQueueName string
	)

	const (
		rpcQueueNamePrefix = "rpc-e2e-test"
	)

	BeforeEach(func(ctx SpecContext) {
		var err error
		clientConn, err = rabbitmqamqp.Dial(ctx, "amqp://localhost:5672", &rabbitmqamqp.AmqpConnOptions{
			Properties: map[string]any{"connection_name": "rpc client e2e test"},
		})
		Ω(err).ShouldNot(HaveOccurred())
		serverConn, err = rabbitmqamqp.Dial(ctx, "amqp://localhost:5672", &rabbitmqamqp.AmqpConnOptions{
			Properties: map[string]any{"connection_name": "rpc server e2e test"},
		})
		Ω(err).ShouldNot(HaveOccurred())

		rpcQueueName = fmt.Sprintf("%s_%s_%d", rpcQueueNamePrefix, CurrentSpecReport().LeafNodeText, GinkgoParallelProcess())
		_, err = serverConn.Management().DeclareQueue(ctx, &rabbitmqamqp.ClassicQueueSpecification{
			Name:         rpcQueueName,
			IsAutoDelete: true,
		})
		Ω(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		_ = rpcClient.Close(ctx)
		_ = rpcServer.Close(ctx)
		Ω(clientConn.Close(ctx)).Should(Succeed())
		Ω(serverConn.Close(ctx)).Should(Succeed())
	})

	It("should work with minimal options", func(ctx SpecContext) {
		m := sync.Mutex{}
		messagesReceivedByServer := 0
		var err error
		rpcClient, err = clientConn.NewRequester(ctx, &rabbitmqamqp.RequesterOptions{
			RequestQueueName: rpcQueueName,
		})
		Ω(err).ShouldNot(HaveOccurred())
		rpcServer, err = serverConn.NewRpcServer(ctx, rabbitmqamqp.RpcServerOptions{
			RequestQueue: rpcQueueName,
			Handler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
				m.Lock()
				messagesReceivedByServer += 1
				m.Unlock()
				reply, err := rabbitmqamqp.NewMessageWithAddress([]byte{}, &rabbitmqamqp.QueueAddress{
					Queue: *request.Properties.ReplyTo,
				})
				if err != nil {
					panic(err)
				}
				x := request.ApplicationProperties["x"].(int64) // int64 because codec encodes int as int64
				reply.ApplicationProperties = map[string]any{"fib": fibonacci(int(x)), "x": x}
				return reply, nil
			},
		})
		Ω(err).ShouldNot(HaveOccurred())

		By("sending and waiting sequentially")
		var expectedFibonacciNumbers [10]int = [10]int{1, 1, 2, 3, 5, 8, 13, 21, 34, 55}
		for i := 1; i <= 10; i++ {
			msg := rpcClient.Message([]byte{})
			msg.ApplicationProperties = map[string]any{"x": i}
			pendingRequestCh, err := rpcClient.Publish(ctx, msg)
			Ω(err).ShouldNot(HaveOccurred())
			select {
			case m := <-pendingRequestCh:
				Ω(m.ApplicationProperties["fib"]).Should(BeEquivalentTo(expectedFibonacciNumbers[i-1]))
				Ω(m.ApplicationProperties["x"]).Should(BeEquivalentTo(i))
			case <-ctx.Done():
				Fail(ctx.Err().Error())
			}
		}
		Expect(messagesReceivedByServer).To(Equal(10))

		By("sending a batch and receiving replies")
		responseChans := make([]<-chan *amqp.Message, 0, 10)
		for i := 1; i <= 10; i++ {
			msg := rpcClient.Message([]byte{})
			msg.ApplicationProperties = map[string]any{"x": i}
			ch, err := rpcClient.Publish(ctx, msg)
			Ω(err).ShouldNot(HaveOccurred())
			responseChans = append(responseChans, ch)
		}

		for i, ch := range responseChans {
			select {
			case m := <-ch:
				Ω(m.ApplicationProperties["fib"]).Should(BeEquivalentTo(expectedFibonacciNumbers[i]))
				Ω(m.ApplicationProperties["x"]).Should(BeEquivalentTo(i + 1))
			case <-ctx.Done():
				Fail(ctx.Err().Error())
			}
		}
		Expect(messagesReceivedByServer).To(Equal(20))

		Expect(rpcClient.Close(ctx)).To(Succeed())
		Expect(rpcServer.Close(ctx)).To(Succeed())
	})
})

func ExampleRpcClient() {
	// open a connection
	conn, err := rabbitmqamqp.Dial(context.TODO(), "amqp://localhost:5672", &rabbitmqamqp.AmqpConnOptions{
		Properties: map[string]any{"connection_name": "example rpc client"},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.TODO())

	// Create RPC client options
	// RequestQueueName is mandatory. The queue must exist.
	options := rabbitmqamqp.RequesterOptions{
		RequestQueueName: "rpc-queue",
	}
	// Create a new RPC client
	rpcClient, err := conn.NewRequester(context.TODO(), &options)
	if err != nil {
		panic(err)
	}
	defer rpcClient.Close(context.TODO())

	// Create an AMQP message with some initial data
	msg := rpcClient.Message([]byte("hello world"))
	// Add some application properties to the message
	msg.ApplicationProperties = map[string]any{"example": "rpc"}

	// Send the message to the server
	pendingRequestCh, err := rpcClient.Publish(context.TODO(), msg)
	if err != nil {
		panic(err)
	}

	// Wait for the reply from the server
	replyFromServer := <-pendingRequestCh
	// Print the reply from the server
	// This example assumes that the server is an "echo" server, that just returns the message it received.
	fmt.Printf("application property 'example': %s\n", replyFromServer.ApplicationProperties["example"])
	fmt.Printf("reply correlation ID: %s\n", replyFromServer.Properties.CorrelationID)
}

type fooCorrelationIdSupplier struct {
	count int
}

func (c *fooCorrelationIdSupplier) Get() any {
	c.count++
	return fmt.Sprintf("foo-%d", c.count)
}

func ExampleRpcClient_customCorrelationID() {
	// type fooCorrelationIdSupplier struct {
	// 	count int
	// }
	//
	// func (c *fooCorrelationIdSupplier) Get() any {
	// 	c.count++
	// 	return fmt.Sprintf("foo-%d", c.count)
	// }

	// Connection setup
	conn, _ := rabbitmqamqp.Dial(context.TODO(), "amqp://", nil)
	defer conn.Close(context.TODO())

	// Create RPC client options
	options := rabbitmqamqp.RequesterOptions{
		RequestQueueName:      "rpc-queue", // the queue must exist
		CorrelationIdSupplier: &fooCorrelationIdSupplier{},
	}

	// Create a new RPC client
	rpcClient, _ := conn.NewRequester(context.TODO(), &options)
	pendingRequestCh, _ := rpcClient.Publish(context.TODO(), rpcClient.Message([]byte("hello world")))
	replyFromServer := <-pendingRequestCh
	fmt.Printf("reply correlation ID: %s\n", replyFromServer.Properties.CorrelationID)
	// Should print: foo-1
}
