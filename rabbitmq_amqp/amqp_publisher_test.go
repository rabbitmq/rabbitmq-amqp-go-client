package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target Publisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target Publisher")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		dest, _ := QueueAddress(&qName)
		publisher, err := connection.Publisher(context.Background(), dest, "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		Expect(publisher).To(BeAssignableToTypeOf(&Publisher{}))

		outcome, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(outcome).NotTo(BeNil())
		Expect(outcome.DeliveryState).To(Equal(&amqp.StateAccepted{}))

		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
	})

	It("Publisher should fail to a not existing exchange", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchangeName := "Nope"
		addr, err := ExchangeAddress(&exchangeName, nil)
		Expect(err).To(BeNil())
		publisher, err := connection.Publisher(context.Background(), addr, "test")
		Expect(err).NotTo(BeNil())
		Expect(publisher).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Outcome should released to a not existing routing key", func() {
		eName := generateNameWithDateTime("Outcome should released to a not existing routing key")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchange, err := connection.Management().DeclareExchange(context.Background(), &ExchangeSpecification{
			Name:         eName,
			IsAutoDelete: false,
			ExchangeType: ExchangeType{Type: Topic},
		})
		Expect(err).To(BeNil())
		Expect(exchange).NotTo(BeNil())
		routingKeyNope := "I don't exist"
		addr, err := ExchangeAddress(&eName, &routingKeyNope)
		Expect(err).To(BeNil())
		publisher, err := connection.Publisher(context.Background(), addr, "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		outcome, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(outcome).NotTo(BeNil())
		Expect(outcome.DeliveryState).To(Equal(&amqp.StateReleased{}))
		Expect(connection.Management().DeleteExchange(context.Background(), eName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})
})
