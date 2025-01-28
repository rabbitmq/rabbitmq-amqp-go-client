package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target NewTargetPublisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target NewTargetPublisher")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		publisher, err := connection.NewTargetPublisher(context.Background(), &QueueAddress{Queue: qName}, "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		Expect(publisher).To(BeAssignableToTypeOf(&TargetPublisher{}))

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

	It("NewTargetPublisher should fail to a not existing exchange", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchangeName := "Nope"
		publisher, err := connection.NewTargetPublisher(context.Background(), &ExchangeAddress{Exchange: exchangeName}, "test")
		Expect(err).NotTo(BeNil())
		Expect(publisher).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("publishResult should released to a not existing routing key", func() {
		eName := generateNameWithDateTime("publishResult should released to a not existing routing key")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchange, err := connection.Management().DeclareExchange(context.Background(), &TopicExchangeSpecification{
			Name:         eName,
			IsAutoDelete: false,
		})
		Expect(err).To(BeNil())
		Expect(exchange).NotTo(BeNil())
		routingKeyNope := "I don't exist"
		Expect(err).To(BeNil())
		publisher, err := connection.NewTargetPublisher(context.Background(), &ExchangeAddress{
			Exchange: eName,
			Key:      routingKeyNope,
		}, "test")
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
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		publisher, err := connection.NewTargetPublisher(context.Background(), &QueueAddress{Queue: qName}, "test")
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

	It("Multi Targets Publisher should fail with StateReleased when the destination does not exist", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		publisher, err := connection.NewMultiTargetsPublisher(context.Background(), "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		qName := generateNameWithDateTime("Targets Publisher should fail when the destination does not exist")
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), &QueueAddress{Queue: qName})
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateReleased{}))
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Multi Targets Publisher should success with StateReceived when the destination exists", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		Expect(err).To(BeNil())
		publisher, err := connection.NewMultiTargetsPublisher(context.Background(), "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		name := generateNameWithDateTime("Targets Publisher should success with StateReceived when the destination exists")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: name,
		})
		Expect(err).To(BeNil())
		// as first message is sent to a queue, the outcome should be StateReceived
		// since the message was accepted by the existing queue
		publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), &QueueAddress{Queue: name})
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))

		_, err = connection.Management().DeclareExchange(context.Background(), &TopicExchangeSpecification{
			Name:         name,
			IsAutoDelete: false,
		})

		// the status should be StateReleased since the exchange does not have any binding
		publishResult, err = publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), &ExchangeAddress{Exchange: name})
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateReleased{}))

		// Create the binding between the exchange and the queue
		_, err = connection.Management().Bind(context.Background(), &BindingSpecification{
			SourceExchange:   name,
			DestinationQueue: name,
			BindingKey:       "#",
		})
		Expect(err).To(BeNil())
		// the status should be StateAccepted since the exchange has a binding
		publishResult, err = publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), &ExchangeAddress{Exchange: name})
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		Expect(connection.Management().DeleteQueue(context.Background(), name)).To(BeNil())
		Expect(connection.Management().DeleteExchange(context.Background(), name)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

})
