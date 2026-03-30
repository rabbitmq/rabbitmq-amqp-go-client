package rabbitmqamqp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target NewPublisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target NewPublisher")
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		exchangeName := "Nope"
		publisher, err := connection.NewPublisher(context.Background(), &ExchangeAddress{Exchange: exchangeName}, nil)
		Expect(err).NotTo(BeNil())
		Expect(publisher).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("NewPublisher should fail for a non-existing queue", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		queueName := generateNameWithDateTime("NonExistingQueue")

		// Try to create a publisher to a queue that doesn't exist
		// This MUST fail at the NewPublisher step
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: queueName}, nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("amqp:not-found"))
		Expect(err.Error()).To(ContainSubstring("no queue"))
		Expect(publisher).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("publishResult should released to a not existing routing key", func() {
		eName := generateNameWithDateTime("publishResult should released to a not existing routing key")
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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

	It("Message should durable by default", func() {
		// https://github.com/rabbitmq/rabbitmq-server/pull/13918

		// Here we test the default behavior of the message durability
		// The lib should set the Header.Durable to true by default
		// when the Header is set by the user
		// it is up to the user to set the Header.Durable to true or false
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		name := generateNameWithDateTime("Message should durable by default")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: name,
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: name}, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())

		msg := NewMessage([]byte("hello"))
		Expect(msg.Header).To(BeNil())
		publishResult, err := publisher.Publish(context.Background(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateAccepted{}))
		Expect(msg.Header).NotTo(BeNil())
		Expect(msg.Header.Durable).To(BeTrue())

		consumer, err := connection.NewConsumer(context.Background(), name, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		dc, err := consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc).NotTo(BeNil())
		Expect(dc.Message().Header).NotTo(BeNil())
		Expect(dc.Message().Header.Durable).To(BeTrue())
		Expect(dc.Accept(context.Background())).To(BeNil())

		msgNotPersistent := NewMessage([]byte("hello"))
		msgNotPersistent.Header = &amqp.MessageHeader{
			Durable: false,
		}
		publishResult, err = publisher.Publish(context.Background(), msgNotPersistent)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&StateAccepted{}))
		Expect(msgNotPersistent.Header).NotTo(BeNil())
		Expect(msgNotPersistent.Header.Durable).To(BeFalse())
		dc, err = consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc).NotTo(BeNil())
		Expect(dc.Message().Header).NotTo(BeNil())
		Expect(dc.Message().Header.Durable).To(BeFalse())
		Expect(dc.Accept(context.Background())).To(BeNil())
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), name)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())

	})
})

var _ = Describe("AMQP publisher async", func() {
	It("PublishAsync should send a message and invoke the callback with StateAccepted", func() {
		qName := generateNameWithDateTime("PublishAsync should send and callback accepted")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName}, nil)
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())

		var wg sync.WaitGroup
		wg.Add(1)
		var callbackResult *PublishResult
		var callbackErr error

		err = publisher.PublishAsync(context.Background(), NewMessage([]byte("hello async")),
			func(result *PublishResult, cbErr error) {
				defer wg.Done()
				callbackResult = result
				callbackErr = cbErr
			})
		Expect(err).To(BeNil())

		wg.Wait()
		Expect(callbackErr).To(BeNil())
		Expect(callbackResult).NotTo(BeNil())
		Expect(callbackResult.Outcome).To(Equal(&amqp.StateAccepted{}))

		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync should send multiple messages and invoke all callbacks", func() {
		qName := generateNameWithDateTime("PublishAsync should send multiple messages")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName}, nil)
		Expect(err).To(BeNil())

		totalMessages := 50
		var wg sync.WaitGroup
		wg.Add(totalMessages)
		var accepted atomic.Int32

		for i := 0; i < totalMessages; i++ {
			err = publisher.PublishAsync(context.Background(), NewMessage([]byte("msg")),
				func(result *PublishResult, cbErr error) {
					defer wg.Done()
					if cbErr == nil && result != nil {
						if _, ok := result.Outcome.(*amqp.StateAccepted); ok {
							accepted.Add(1)
						}
					}
				})
			Expect(err).To(BeNil())
		}

		wg.Wait()
		Expect(int(accepted.Load())).To(Equal(totalMessages))

		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(totalMessages))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync should respect MaxInFlight back-pressure", func() {
		qName := generateNameWithDateTime("PublishAsync should respect MaxInFlight")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())

		maxInFlight := 5
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName},
			&PublisherOptions{MaxInFlight: maxInFlight})
		Expect(err).To(BeNil())

		totalMessages := 20
		var wg sync.WaitGroup
		wg.Add(totalMessages)
		var accepted atomic.Int32

		for i := 0; i < totalMessages; i++ {
			err = publisher.PublishAsync(context.Background(), NewMessage([]byte("msg")),
				func(result *PublishResult, cbErr error) {
					defer wg.Done()
					if cbErr == nil && result != nil {
						if _, ok := result.Outcome.(*amqp.StateAccepted); ok {
							accepted.Add(1)
						}
					}
				})
			Expect(err).To(BeNil())
		}

		wg.Wait()
		Expect(int(accepted.Load())).To(Equal(totalMessages))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync should return error when context is cancelled while waiting for in-flight slot", func() {
		qName := generateNameWithDateTime("PublishAsync context cancelled on backpressure")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())

		// MaxInFlight = 1 so only one goroutine can be waiting at a time
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName},
			&PublisherOptions{MaxInFlight: 1, PublishTimeout: 30 * time.Second})
		Expect(err).To(BeNil())

		var wg sync.WaitGroup
		wg.Add(1)

		// Fill the single in-flight slot with a message whose callback blocks
		blocker := make(chan struct{})
		err = publisher.PublishAsync(context.Background(), NewMessage([]byte("blocker")),
			func(result *PublishResult, cbErr error) {
				defer wg.Done()
				<-blocker // hold the slot
			})
		Expect(err).To(BeNil())

		// Now try to publish with an already-cancelled context; should fail immediately
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = publisher.PublishAsync(cancelledCtx, NewMessage([]byte("should fail")),
			func(result *PublishResult, cbErr error) {})
		Expect(err).NotTo(BeNil())
		Expect(err).To(Equal(context.Canceled))

		close(blocker)
		wg.Wait()

		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync should fail with TO error for dynamic target without TO property", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), nil, nil)
		Expect(err).To(BeNil())

		err = publisher.PublishAsync(context.Background(), NewMessage([]byte("hello")),
			func(result *PublishResult, cbErr error) {})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("message properties TO is required"))

		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync callback should receive StateReleased for unroutable exchange message", func() {
		eName := generateNameWithDateTime("PublishAsync released for unroutable")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		_, err = connection.Management().DeclareExchange(context.Background(), &TopicExchangeSpecification{
			Name:         eName,
			IsAutoDelete: false,
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &ExchangeAddress{
			Exchange: eName,
			Key:      "no-binding-key",
		}, nil)
		Expect(err).To(BeNil())

		var wg sync.WaitGroup
		wg.Add(1)
		var callbackResult *PublishResult
		var callbackErr error

		err = publisher.PublishAsync(context.Background(), NewMessage([]byte("hello")),
			func(result *PublishResult, cbErr error) {
				defer wg.Done()
				callbackResult = result
				callbackErr = cbErr
			})
		Expect(err).To(BeNil())

		wg.Wait()
		Expect(callbackErr).To(BeNil())
		Expect(callbackResult).NotTo(BeNil())
		Expect(callbackResult.Outcome).To(Equal(&StateReleased{}))

		Expect(connection.Management().DeleteExchange(context.Background(), eName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("PublishAsync with custom PublishTimeout should work", func() {
		qName := generateNameWithDateTime("PublishAsync custom timeout")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: qName},
			&PublisherOptions{PublishTimeout: 10 * time.Second})
		Expect(err).To(BeNil())

		var wg sync.WaitGroup
		wg.Add(1)
		var callbackResult *PublishResult
		var callbackErr error

		err = publisher.PublishAsync(context.Background(), NewMessage([]byte("hello timeout")),
			func(result *PublishResult, cbErr error) {
				defer wg.Done()
				callbackResult = result
				callbackErr = cbErr
			})
		Expect(err).To(BeNil())

		wg.Wait()
		Expect(callbackErr).To(BeNil())
		Expect(callbackResult).NotTo(BeNil())
		Expect(callbackResult.Outcome).To(Equal(&amqp.StateAccepted{}))

		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})
})
