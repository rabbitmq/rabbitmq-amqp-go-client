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

var _ = Describe("RpcServer E2E", func() {
	It("process incoming message requests", func() {
		// setup
		processedMessage := make(chan string, 5)

		requestQueue := generateNameWithDateTime(CurrentSpecReport().LeafNodeText)
		c, err := declareQueueAndConnection(requestQueue)
		Expect(err).ToNot(HaveOccurred())
		defer c.Close(context.Background())

		replyQueue, err := c.management.DeclareQueue(
			context.Background(),
			&ClassicQueueSpecification{Name: "reply-queue", IsAutoDelete: true},
		)
		Expect(err).ToNot(HaveOccurred())
		replyConsumer, err := c.NewConsumer(context.Background(), replyQueue.Name(), nil)
		Expect(err).ToNot(HaveOccurred())
		defer replyConsumer.Close(context.Background())
		requestPublisher, err := c.NewPublisher(context.Background(), &QueueAddress{Queue: requestQueue}, nil)
		Expect(err).ToNot(HaveOccurred())
		defer requestPublisher.Close(context.Background())

		publisher, err := c.NewPublisher(context.Background(), nil, nil)
		Expect(err).ToNot(HaveOccurred())

		consumer, err := c.NewConsumer(context.Background(), requestQueue, nil)
		Expect(err).ToNot(HaveOccurred())

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
		}
		defer server.Close(context.Background())
		go server.handle()

		// act
		message := amqp.NewMessage([]byte("message 1"))
		message.Properties = &amqp.MessageProperties{
			MessageID: "1",
			ReplyTo:   ptr(fmt.Sprintf("/queues/%s", replyQueue.Name())),
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := requestPublisher.Publish(ctx, message)
		Expect(err).ToNot(HaveOccurred())
		Expect(res.Outcome).To(BeAssignableToTypeOf(&StateAccepted{}), "expected rabbit to confirm the message")

		// assert
		Eventually(processedMessage).Within(time.Second).Should(Receive(Equal("1")))
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		defer cancel2()
		serverReply, err := replyConsumer.Receive(ctx2)
		Expect(err).ToNot(HaveOccurred())
		m := serverReply.Message()
		Expect(m).ToNot(BeNil())
		Expect(m.GetData()).To(BeEquivalentTo("reply"))
		Expect(m.Properties.CorrelationID).To(BeEquivalentTo("1"))
	})

	It("stops the handler when the RPC server closes", Focus, func(ctx SpecContext) {
		// setup
		requestQueue := generateNameWithDateTime(CurrentSpecReport().LeafNodeText)
		c, err := declareQueueAndConnection(requestQueue)
		Expect(err).ToNot(HaveOccurred())
		defer c.Close(ctx)

		publisher, err := c.NewPublisher(ctx, nil, nil)
		Expect(err).ToNot(HaveOccurred())

		consumer, err := c.NewConsumer(ctx, requestQueue, nil)
		Expect(err).ToNot(HaveOccurred())

		server := &amqpRpcServer{
			publisher: publisher,
			consumer:  consumer,
			requestHandler: func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
				return nil, nil
			},
		}
		go server.handle()

		buf := gbytes.NewBuffer()
		SetSlogHandler(NewGinkgoHandler(slog.LevelDebug, buf))
		server.Close(ctx)

		Eventually(buf).Within(time.Second).Should(gbytes.Say("RPC server is closed. Stopping the handler"))
	}, SpecTimeout(time.Second*10))
})
