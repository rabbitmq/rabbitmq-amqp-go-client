package rabbitmqamqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"time"
)

var _ = Describe("NewConsumer tests", func() {

	It("AMQP NewConsumer should fail due to context cancellation", func() {
		qName := generateNameWithDateTime("AMQP NewConsumer should fail due to context cancellation")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())

		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
		cancel()
		_, err = connection.NewConsumer(ctx, qName, nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("context canceled"))
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP NewConsumer should ack and empty the queue", func() {
		qName := generateNameWithDateTime("AMQP NewConsumer should ack and empty the queue")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())
		publishMessages(qName, 10)
		consumer, err := connection.NewConsumer(context.Background(), qName, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(dc.Accept(context.Background())).To(BeNil())
		}
		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(0))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP NewConsumer should requeue the message to the queue", func() {

		qName := generateNameWithDateTime("AMQP NewConsumer should requeue the message to the queue")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())
		publishMessages(qName, 1)
		consumer, err := connection.NewConsumer(context.Background(), qName, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
		dc, err := consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc.Message()).NotTo(BeNil())
		Expect(dc.Requeue(context.Background())).To(BeNil())
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(err).To(BeNil())
		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP NewConsumer should requeue the message to the queue with annotations", func() {

		qName := generateNameWithDateTime("AMQP NewConsumer should requeue the message to the queue with annotations")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())
		publishMessages(qName, 1)
		consumer, err := connection.NewConsumer(context.Background(), qName, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
		dc, err := consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc.Message()).NotTo(BeNil())
		myAnnotations := amqp.Annotations{
			"x-key1": "value1",
			"x-key2": "value2",
		}
		Expect(dc.RequeueWithAnnotations(context.Background(), myAnnotations)).To(BeNil())
		dcWithAnnotation, err := consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dcWithAnnotation.Message().Annotations["x-key1"]).To(Equal("value1"))
		Expect(dcWithAnnotation.Message().Annotations["x-key2"]).To(Equal("value2"))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(err).To(BeNil())
		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP NewConsumer should discard the message to the queue with and without annotations", func() {
		// TODO: Implement this test with a dead letter queue to test the discard feature
		qName := generateNameWithDateTime("AMQP NewConsumer should discard the message to the queue with and without annotations")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())
		publishMessages(qName, 2)
		consumer, err := connection.NewConsumer(context.Background(), qName, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
		dc, err := consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc.Message()).NotTo(BeNil())
		myAnnotations := amqp.Annotations{
			"x-key1": "value1",
			"x-key2": "value2",
		}
		Expect(dc.DiscardWithAnnotations(context.Background(), myAnnotations)).To(BeNil())
		dc, err = consumer.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc.Message()).NotTo(BeNil())
		Expect(dc.Discard(context.Background(), &amqp.Error{
			Condition:   "my error",
			Description: "my error description",
			Info:        nil,
		})).To(BeNil())
		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(0))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

})
