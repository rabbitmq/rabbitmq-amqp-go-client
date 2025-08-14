package rabbitmqamqp

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RpcClient", func() {
	var (
		conn      *AmqpConnection
		queueName string
		consumer  *Consumer
		publisher *Publisher
	)

	var pongRpcServer = func(ctx context.Context, publisher *Publisher, consumer *Consumer) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Receive a message from the server consumer
				receivedMessage, err := consumer.Receive(ctx)
				if err != nil {
					// Exit if we can't receive messages (e.g.,
					// consumer is closed)
					GinkgoWriter.Printf("Error receiving message: %v\n", err)
					return
				}

				msg := receivedMessage.Message()
				if msg == nil {
					GinkgoWriter.Printf("Received nil message\n")
					continue
				}

				// Create response with "Pong: " prefix
				responseData := "Pong: " + string(msg.GetData())
				replyMessage := amqp.NewMessage([]byte(responseData))

				// Copy correlation ID and reply-to from request
				if msg.Properties != nil {
					if replyMessage.Properties == nil {
						replyMessage.Properties = &amqp.MessageProperties{}
					}
					replyMessage.Properties.CorrelationID =
						msg.Properties.MessageID
				}

				// Send reply to the specified reply-to address
				if msg.Properties != nil && msg.Properties.ReplyTo != nil {
					replyMessage.Properties.To = msg.Properties.ReplyTo
				}

				copyApplicationProperties(msg, replyMessage)

				publisher.Publish(ctx, replyMessage)
			}
		}
	}

	BeforeEach(func() {
		queueName = generateNameWithDateTime(CurrentSpecReport().LeafNodeText)
		var err error
		conn, err = declareQueueAndConnection(queueName)
		Expect(err).ToNot(HaveOccurred())
		consumer, err = conn.NewConsumer(context.Background(), queueName, nil)
		Expect(err).ToNot(HaveOccurred())
		publisher, err = conn.NewPublisher(context.Background(), nil, nil)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		_ = consumer.Close(context.Background())
		_ = publisher.Close(context.Background())
		_ = conn.Close(context.Background())
	})

	It("should send a request and receive replies", func(ctx SpecContext) {
		// Server goroutine to handle incoming requests
		go pongRpcServer(ctx, publisher, consumer)

		client, err := conn.NewRpcClient(ctx, &RpcClientOptions{
			RequestQueueName: queueName,
		})
		Ω(err).ShouldNot(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			// Closing twice in case the test fails and the 'happy path' close is not called
			_ = client.Close(ctx)
		})

		for i := 0; i < 10; i++ {
			m := client.Message([]byte(fmt.Sprintf("Message %d", i)))
			replyCh, err := client.Publish(ctx, m)
			Ω(err).ShouldNot(HaveOccurred())
			actualReply := &amqp.Message{}
			Eventually(replyCh).
				Within(time.Second).
				WithPolling(time.Millisecond * 100).
				Should(Receive(&actualReply))
			Expect(actualReply.GetData()).To(BeEquivalentTo(fmt.Sprintf("Pong: Message %d", i)))
		}
		Ω(client.Close(ctx)).Should(Succeed())
	})

	It("uses a custom correlation id extractor and post processor", func(ctx SpecContext) {
		go pongRpcServer(ctx, publisher, consumer)
		client, err := conn.NewRpcClient(ctx, &RpcClientOptions{
			RequestQueueName: queueName,
			CorrelationIdExtractor: func(message *amqp.Message) any {
				return message.ApplicationProperties["correlationId"]
			},
			RequestPostProcessor: func(request *amqp.Message, correlationID any) *amqp.Message {
				if request.ApplicationProperties == nil {
					request.ApplicationProperties = make(map[string]any)
				}
				request.ApplicationProperties["correlationId"] = correlationID
				if request.Properties == nil {
					request.Properties = &amqp.MessageProperties{}
				}
				request.Properties.MessageID = correlationID

				return request
			},
		})
		Ω(err).ShouldNot(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			// Closing twice in case the test fails and the 'happy path' close is not called
			_ = client.Close(ctx)
		})

		request := client.Message([]byte("Using a custom correlation id extractor and post processor"))
		request.ApplicationProperties = map[string]any{"this-property": "should-be-preserved"}
		replyCh, err := client.Publish(ctx, request)
		Ω(err).ShouldNot(HaveOccurred())

		actualReply := &amqp.Message{}
		Eventually(replyCh).
			Within(time.Second).
			WithPolling(time.Millisecond * 100).
			Should(Receive(&actualReply))
		Expect(actualReply.GetData()).To(BeEquivalentTo("Pong: Using a custom correlation id extractor and post processor"))
		Expect(actualReply.ApplicationProperties).To(HaveKey("correlationId"))
		Expect(actualReply.ApplicationProperties).To(HaveKeyWithValue("this-property", "should-be-preserved"))
		Ω(client.Close(ctx)).Should(Succeed())
	})
})
