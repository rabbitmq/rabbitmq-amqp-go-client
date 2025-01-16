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

		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))

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

	It("Publisher should fail if the destination address does not start in the correct way", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		destinationAddress := "this is not valid since does not start with exchanges or queues"
		Expect(err).To(BeNil())
		publisher, err := connection.Publisher(context.Background(), destinationAddress, "test")
		Expect(err).NotTo(BeNil())
		Expect(publisher).To(BeNil())
		Expect(err.Error()).To(ContainSubstring("invalid destination address"))
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("publishResult should released to a not existing routing key", func() {
		eName := generateNameWithDateTime("publishResult should released to a not existing routing key")
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
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateReleased{}))
		Expect(connection.Management().DeleteExchange(context.Background(), eName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Send a message to a deleted queue should fail", func() {
		qName := generateNameWithDateTime("Send a message to a deleted queue should fail")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		dest, _ := QueueAddress(&qName)
		publisher, err := connection.Publisher(context.Background(), dest, "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		err = connection.management.DeleteQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		publishResult, err = publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))
		Expect(err).NotTo(BeNil())
		Expect(connection.Close(context.Background()))
	})
})
