package rabbitmqamqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target NewPublisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target NewPublisher")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName}, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		Expect(publisher).To(BeAssignableToTypeOf(&Publisher{}))

		publishResult, err := publisher.Publish(context.Background(), NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))

		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
	})

	It("NewPublisher should fail to a not existing exchange", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchangeName := "Nope"
		publisher, err := connection.NewPublisher(context.Background(), &ExchangeAddress{Exchange: exchangeName}, nil)
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
		publisher, err := connection.NewPublisher(context.Background(), &ExchangeAddress{
			Exchange: eName,
			Key:      routingKeyNope,
		}, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		publishResult, err := publisher.Publish(context.Background(), NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateReleased{}))
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
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName}, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		publishResult, err := publisher.Publish(context.Background(), NewMessage([]byte("hello")))
		Expect(err).To(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateAccepted{}))
		err = connection.management.DeleteQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		publishResult, err = publisher.Publish(context.Background(), NewMessage([]byte("hello")))
		Expect(publishResult).To(BeNil())
		Expect(err).NotTo(BeNil())
		Expect(connection.Close(context.Background()))
	})

	It("Multi Targets NewPublisher should fail with StateReleased when the destination does not exist", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), nil, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		qName := generateNameWithDateTime("Targets NewPublisher should fail when the destination does not exist")
		msg := amqp.NewMessage([]byte("hello"))
		Expect(MessagePropertyToAddress(msg, &QueueAddress{Queue: qName})).To(BeNil())
		publishResult, err := publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateReleased{}))
		msg, err = NewMessageWithAddress([]byte("hello"), &QueueAddress{Queue: qName})
		Expect(err).To(BeNil())
		publishResult, err = publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateReleased{}))

		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Multi Targets NewPublisher should success with StateReceived when the destination exists", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		Expect(err).To(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), nil, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		name := generateNameWithDateTime("Targets NewPublisher should success with StateReceived when the destination exists")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: name,
		})
		Expect(err).To(BeNil())
		// as first message is sent to a queue, the outcome should be StateReceived
		// since the message was accepted by the existing queue
		msg := NewMessage([]byte("hello"))
		Expect(MessagePropertyToAddress(msg, &QueueAddress{Queue: name})).To(BeNil())

		publishResult, err := publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateAccepted{}))

		_, err = connection.Management().DeclareExchange(context.Background(), &TopicExchangeSpecification{
			Name:         name,
			IsAutoDelete: false,
		})
		Expect(err).To(BeNil())

		msg = NewMessage([]byte("hello"))
		Expect(MessagePropertyToAddress(msg, &ExchangeAddress{Exchange: name})).To(BeNil())
		// the status should be StateReleased since the exchange does not have any binding
		publishResult, err = publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateReleased{}))

		// Create the binding between the exchange and the queue
		_, err = connection.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
			SourceExchange:   name,
			DestinationQueue: name,
			BindingKey:       "#",
		})
		Expect(err).To(BeNil())
		// the status should be StateAccepted since the exchange has a binding
		msg = NewMessage([]byte("hello"))
		Expect(MessagePropertyToAddress(msg, &ExchangeAddress{Exchange: name})).To(BeNil())
		publishResult, err = publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateAccepted{}))
		Expect(connection.Management().DeleteQueue(context.Background(), name)).To(BeNil())
		Expect(connection.Management().DeleteExchange(context.Background(), name)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Multi Targets NewPublisher should fail it TO is not set or not valid", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), nil, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		msg := NewMessage([]byte("hello"))
		// the message should fail since the TO property is not set
		publishResult, err := publisher.Publish(context.Background(), msg)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("message properties TO is required"))
		Expect(publishResult).To(BeNil())

		invalid := "invalid"
		// the message should fail since the TO property is not valid
		msg.Properties = &amqp.MessageProperties{
			To: &invalid,
		}

		publishResult, err = publisher.Publish(context.Background(), msg)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("invalid destination address"))
		Expect(publishResult).To(BeNil())

		Expect(connection.Close(context.Background())).To(BeNil())
	})
})
