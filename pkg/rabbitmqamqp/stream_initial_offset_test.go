package rabbitmqamqp

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stream initial offset", func() {
	var connection *AmqpConnection

	BeforeEach(func() {
		conn, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		connection = conn
	})

	AfterEach(func() {
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("fails to declare a stream with InitialOffset on RabbitMQ < 4.4", func() {
		if connection.featuresAvailable.is44rMore {
			Skip("only fails on RabbitMQ < 4.4")
		}
		qName := generateNameWithDateTime("stream initial offset unsupported")
		_, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name:          qName,
			InitialOffset: &InitialOffset{Value: 1000},
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("RabbitMQ 4.4"))
	})

	It("fails to declare a stream with a negative InitialOffset", func() {
		qName := generateNameWithDateTime("stream initial offset negative")
		_, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name:          qName,
			InitialOffset: &InitialOffset{Value: -1},
		})
		Expect(err).NotTo(BeNil())
	})

	It("starts the stream offsets from InitialOffset", func() {
		if !connection.featuresAvailable.is44rMore {
			Skip("requires RabbitMQ 4.4+ for stream initial offset")
		}
		const initialOffset = 1000
		const messageCount = 5
		qName := generateNameWithDateTime("stream initial offset")

		_, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name:          qName,
			InitialOffset: &InitialOffset{Value: initialOffset},
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName}, nil)
		Expect(err).To(BeNil())
		for i := 0; i < messageCount; i++ {
			res, err := publisher.Publish(context.Background(), NewMessage([]byte("Hello World")))
			Expect(err).To(BeNil())
			Expect(res.Outcome).To(BeAssignableToTypeOf(&StateAccepted{}))
		}
		Expect(publisher.Close(context.Background())).To(BeNil())

		consumer, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			Offset: &OffsetFirst{},
		})
		Expect(err).To(BeNil())

		for i := 0; i < messageCount; i++ {
			dc, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			off, ok := streamOffsetFromAnnotation(dc.Message().Annotations["x-stream-offset"])
			Expect(ok).To(BeTrue())
			Expect(off).To(BeEquivalentTo(initialOffset + i))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
	})
})
