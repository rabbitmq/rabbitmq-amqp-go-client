package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
	"time"
)

var _ = Describe("Recovery connection test", func() {
	It("connection should reconnect producers and consumers if dropped by via REST API", func() {
		/*
			The test is a bit complex since it requires to drop the connection by REST API
			Then wait for the connection to be reconnected.
			The scope of the test is to verify that the connection is reconnected and the
			producers and consumers are able to send and receive messages.
			It is more like an integration test.
			This kind of the tests requires time in terms of execution it has to wait for the
			connection to be reconnected, so to speed up the test I aggregated the tests in one.
		*/

		name := "connection should reconnect producers and consumers if dropped by via REST API"
		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: name,
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 2 * time.Second,
				MaxReconnectAttempts:     5,
			},
		})
		Expect(err).To(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)

		qName := generateName(name)
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())

		consumer, err := connection.NewConsumer(context.Background(),
			qName, nil)

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{
			Queue: qName,
		}, "test")

		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		for i := 0; i < 5; i++ {
			publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello")))
			Expect(err).To(BeNil())
			Expect(publishResult).NotTo(BeNil())
			Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		}

		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(name)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st1 := <-ch
		Expect(st1.From).To(Equal(&StateOpen{}))
		Expect(st1.To).To(BeAssignableToTypeOf(&StateClosed{}))
		///  Closed state should have an error
		// Since it is forced closed by the REST API
		err = st1.To.(*StateClosed).GetError()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("Connection forced"))

		time.Sleep(1 * time.Second)
		Eventually(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(name)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st2 := <-ch
		Expect(st2.From).To(BeAssignableToTypeOf(&StateClosed{}))
		Expect(st2.To).To(Equal(&StateReconnecting{}))

		st3 := <-ch
		Expect(st3.From).To(BeAssignableToTypeOf(&StateReconnecting{}))
		Expect(st3.To).To(Equal(&StateOpen{}))

		for i := 0; i < 5; i++ {
			publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello")))
			Expect(err).To(BeNil())
			Expect(publishResult).NotTo(BeNil())
			Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		}

		/// after the connection is reconnected the consumer should be able to receive the messages
		for i := 0; i < 10; i++ {
			deliveryContext, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(deliveryContext).NotTo(BeNil())
		}

		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())

		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
		st4 := <-ch
		Expect(st4.From).To(Equal(&StateOpen{}))
		Expect(st4.To).To(BeAssignableToTypeOf(&StateClosed{}))
		err = st4.To.(*StateClosed).GetError()
		// the flow status should be:
		// from open to closed (with error)
		// from closed to reconnecting
		// from reconnecting to open
		// from open to closed (without error)
		Expect(err).To(BeNil())
	})

	It("connection should not reconnect producers and consumers if the auto-recovery is disabled", func() {
		name := "connection should reconnect producers and consumers if dropped by via REST API"
		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: name,
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery: false, // disabled
			},
		})
		Expect(err).To(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)

		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(name)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st1 := <-ch
		Expect(st1.From).To(Equal(&StateOpen{}))
		Expect(st1.To).To(BeAssignableToTypeOf(&StateClosed{}))

		err = st1.To.(*StateClosed).GetError()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("Connection forced"))

		time.Sleep(1 * time.Second)

		// the connection should not be reconnected
		Consistently(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(name)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeFalse())

		err = connection.Close(context.Background())
		Expect(err).NotTo(BeNil())
	})

	It("validate the Recovery connection parameters", func() {

		_, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 500 * time.Millisecond,
				MaxReconnectAttempts:     5,
			},
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("BackOffReconnectInterval should be greater than"))

		_, err = Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:       true,
				MaxReconnectAttempts: 0,
			},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("MaxReconnectAttempts should be greater than"))
	})

})
