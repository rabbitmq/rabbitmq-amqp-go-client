package rabbitmqamqp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = FDescribe("RpcServer E2E", func() {
	var (
		conn         *AmqpConnection
		publisher    *Publisher
		consumer     *Consumer
		requestQueue string
	)

	BeforeEach(func() {
		requestQueue = generateNameWithDateTime(CurrentSpecReport().LeafNodeText)
		var err error
		conn, err = declareQueueAndConnection(requestQueue)
		Expect(err).ToNot(HaveOccurred())

		publisher, err = conn.NewPublisher(context.Background(), nil, nil)
		Expect(err).ToNot(HaveOccurred())

		consumer, err = conn.NewConsumer(context.Background(), requestQueue, nil)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		publisher.Close(ctx)
		consumer.Close(ctx)
		conn.Close(ctx)
	})

	It("process incoming message requests", func(ctx SpecContext) {
		// setup
		processedMessage := make(chan string, 5)

		replyQueue, err := conn.management.DeclareQueue(
			context.Background(),
			&ClassicQueueSpecification{Name: fmt.Sprintf("reply-queue_%s", CurrentSpecReport().LeafNodeText), IsExclusive: true},
		)
		Expect(err).ToNot(HaveOccurred())

		replyConsumer, err := conn.NewConsumer(context.Background(), replyQueue.Name(), nil)
		Expect(err).ToNot(HaveOccurred())
		requestPublisher, err := conn.NewPublisher(context.Background(), &QueueAddress{Queue: requestQueue}, nil)
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			replyConsumer.Close(ctx)
			requestPublisher.Close(ctx)
		})

		server := &amqpRpcServer{
			publisher: publisher,
			consumer:  consumer,
			requestHandler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
				if request.Properties == nil {
					return nil, fmt.Errorf("request properties are nil")
				}
				messageID, ok := request.Properties.MessageID.(string)
				if !ok {
					return nil, fmt.Errorf("correlation ID is not a string")
				}
				processedMessage <- messageID
				reply := amqp.NewMessage([]byte("reply"))
				return reply, nil
			},
			correlationIdExtractor: defaultCorrelationIdExtractor,
			postProcessor:          defaultPostProcessor,
		}
		defer server.Close(context.Background())
		go server.handle()

		// act
		message := amqp.NewMessage([]byte("message 1"))
		q, err := (&QueueAddress{Queue: replyQueue.Name()}).toAddress()
		Expect(err).ToNot(HaveOccurred())
		message.Properties = &amqp.MessageProperties{
			MessageID: "1",
			ReplyTo:   &q,
		}
		res, err := requestPublisher.Publish(ctx, message)
		Expect(err).ToNot(HaveOccurred())
		Expect(res.Outcome).To(BeAssignableToTypeOf(&StateAccepted{}), "expected rabbit to confirm the message")

		// assert
		Eventually(processedMessage).Within(time.Second).Should(Receive(Equal("1")))
		serverReply, err := replyConsumer.Receive(ctx)
		Expect(err).ToNot(HaveOccurred())
		m := serverReply.Message()
		Expect(m).ToNot(BeNil())
		Expect(m.GetData()).To(BeEquivalentTo("reply"))
		Expect(m.Properties.CorrelationID).To(BeEquivalentTo("1"))
	}, SpecTimeout(time.Second*10))

	It("stops the handler when the RPC server closes", func(ctx SpecContext) {
		// setup
		server := &amqpRpcServer{
			publisher: publisher,
			consumer:  consumer,
			requestHandler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
				return nil, nil
			},
			correlationIdExtractor: defaultCorrelationIdExtractor,
			postProcessor:          defaultPostProcessor,
		}
		go server.handle()
		time.Sleep(time.Second) // ugly but necessary to wait for the server to call Receive() and block

		buf := gbytes.NewBuffer()
		SetSlogHandler(NewGinkgoHandler(slog.LevelDebug, buf))

		// act
		server.Close(ctx)

		// assert
		Eventually(buf).Within(time.Second).Should(gbytes.Say("Receive request returned error. This may be expected if the server is closing"))
		Eventually(buf).Within(time.Second).Should(gbytes.Say("RPC server is closed. Stopping the handler"))
	}, SpecTimeout(time.Second*10))

	It("uses a custom correlation id extractor and post processor", func(ctx SpecContext) {
		// setup
		replyQueue, err := conn.management.DeclareQueue(
			context.Background(),
			&ClassicQueueSpecification{Name: fmt.Sprintf("reply-queue_%s", CurrentSpecReport().LeafNodeText), IsExclusive: true},
		)
		Expect(err).ToNot(HaveOccurred())

		replyConsumer, err := conn.NewConsumer(context.Background(), replyQueue.Name(), nil)
		Expect(err).ToNot(HaveOccurred())
		requestPublisher, err := conn.NewPublisher(context.Background(), &QueueAddress{Queue: requestQueue}, nil)
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			replyConsumer.Close(ctx)
			requestPublisher.Close(ctx)
		})

		correlationIdExtractor := func(message *amqp.Message) any {
			return message.ApplicationProperties["message-id"]
		}
		postProcessor := func(reply *amqp.Message, correlationID any) *amqp.Message {
			reply.Properties.CorrelationID = correlationID
			reply.ApplicationProperties["test"] = "success"
			return reply
		}
		server := &amqpRpcServer{
			publisher:              publisher,
			consumer:               consumer,
			correlationIdExtractor: correlationIdExtractor,
			postProcessor:          postProcessor,
			requestHandler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
				m := amqp.NewMessage(request.GetData())
				m.Properties = &amqp.MessageProperties{}
				m.ApplicationProperties = make(map[string]any)
				return m, nil
			},
		}
		go server.handle()

		// act
		message := amqp.NewMessage([]byte("message with custom correlation id extractor and custom post processor"))
		q, err := (&QueueAddress{Queue: replyQueue.Name()}).toAddress()
		Expect(err).ToNot(HaveOccurred())
		message.Properties = &amqp.MessageProperties{
			MessageID: 123,
			ReplyTo:   &q,
		}
		message.ApplicationProperties = map[string]any{
			"message-id": "my-message-id",
		}
		res, err := requestPublisher.Publish(ctx, message)
		Expect(err).ToNot(HaveOccurred())
		Expect(res.Outcome).To(BeAssignableToTypeOf(&StateAccepted{}), "expected rabbit to confirm the message")

		// assert
		serverReply, err := replyConsumer.Receive(ctx)
		Expect(err).ToNot(HaveOccurred())
		m := serverReply.Message()
		Expect(m).ToNot(BeNil())
		Expect(m.GetData()).To(BeEquivalentTo("message with custom correlation id extractor and custom post processor"))
		Expect(m.Properties.CorrelationID).To(BeEquivalentTo("my-message-id"))
		Expect(m.ApplicationProperties["test"]).To(BeEquivalentTo("success"))
	}, SpecTimeout(time.Second*10))
})
