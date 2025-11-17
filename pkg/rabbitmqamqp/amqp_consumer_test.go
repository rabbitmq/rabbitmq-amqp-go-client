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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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

var _ = Describe("Consumer pause and unpause", func() {
	It("pauses and unpauses the consumer", func(ctx SpecContext) {
		// setup
		qName := CurrentSpecReport().LeafNodeText
		c, err := declareQueueAndConnection(qName)
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			_ = c.Close(ctx)
		})

		publishMessages(qName, 1)
		consumer, err := c.NewConsumer(ctx, qName, &ConsumerOptions{InitialCredits: -1})
		Expect(err).ToNot(HaveOccurred())
		Expect(consumer.receiver.Load().IssueCredit(1)).To(Succeed())

		By("receiving a message when unpaused")

		dc, err := consumer.Receive(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(dc.Accept(ctx)).To(Succeed())

		By("not receiving any new messages after pausing")
		Expect(consumer.pause(ctx)).To(Succeed())
		Eventually(consumer.isPausedOrPausing).Should(BeTrue(), "expected consumer to be paused or pausing")
		publishMessages(qName, 10)
		// have to assert again because pause may enocunter an error and not complete the pause operation
		Eventually(consumer.isPausedOrPausing).Should(BeTrue(), "expected consumer to be paused")

		rCtx, rCancel := context.WithTimeout(ctx, 200*time.Millisecond)
		DeferCleanup(rCancel)
		_, err = consumer.Receive(rCtx)
		Expect(err).To(MatchError(context.DeadlineExceeded))

		By("receiving a new message after unpausing")
		Expect(consumer.unpause(10)).To(Succeed())
		Eventually(consumer.isPausedOrPausing).Should(BeFalse(), "expected consumer to be unpaused")

		for i := 0; i < 10; i++ {
			dc, err = consumer.Receive(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(dc.Accept(ctx)).To(Succeed())
		}
	}, SpecTimeout(time.Second*10))
})

var _ = Describe("Consumer direct reply to", func() {
	It("Queue address should be the same passed by the user", func() {
		qName := generateNameWithDateTime("Queue address should be the same passed by the user")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queue).NotTo(BeNil())

		consumer, err := connection.NewConsumer(context.Background(), qName, &ConsumerOptions{})
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		q := &QueueAddress{Queue: qName}
		r, e := q.toAddress()
		Expect(e).To(BeNil())
		qc, err := consumer.GetQueue()
		Expect(err).To(BeNil())
		Expect(r).To(Equal(qc))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Queue address should be the dynamic name containing amq.rabbitmq.reply-to", func() {

		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())

		consumer, err := connection.NewConsumer(context.Background(), "", &ConsumerOptions{
			DirectReplyTo: true,
		})
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		addr, err := consumer.GetQueue()
		Expect(err).To(BeNil())
		Expect(addr).NotTo(ContainSubstring("/queues/"))
		Expect(addr).To(ContainSubstring("amq.rabbitmq.reply-to"))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Validate consumer queues with special characters", func() {
		type ExpectedQueuesAndDestination struct {
			Queue   string
			Address string
		}
		e := []ExpectedQueuesAndDestination{
			{"queue with spaces", "/queues/queue%20with%20spaces"},
			{"queue+with+plus", "/queues/queue%2Bwith%2Bplus"},
			{"特殊字符", "/queues/%E7%89%B9%E6%AE%8A%E5%AD%97%E7%AC%A6"},
			{"myQueue", "/queues/myQueue"},
			{"queue/with/slash", "/queues/queue%2Fwith%2Fslash"},
			{"queue?with?question", "/queues/queue%3Fwith%3Fquestion"},
			{"emoji😊queue", "/queues/emoji%F0%9F%98%8Aqueue"},
			{"!@#$%^&*()", "/queues/%21%40%23%24%25%5E%26%2A%28%29"},
		}
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		for i := range e {
			queue, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
				Name: e[i].Queue,
			})
			Expect(err).To(BeNil())
			Expect(queue).NotTo(BeNil())

			consumer, err := connection.NewConsumer(context.Background(), e[i].Queue, nil)
			Expect(err).To(BeNil())
			Expect(consumer).NotTo(BeNil())
			qc, err := consumer.GetQueue()
			Expect(err).To(BeNil())
			Expect(qc).To(Equal(e[i].Queue))
			Expect(consumer.Close(context.Background())).To(BeNil())
			Expect(consumer.destinationAdd).To(Equal(e[i].Address))
			Expect(connection.Management().DeleteQueue(context.Background(), e[i].Queue)).To(BeNil())
		}
		Expect(connection.Close(context.Background())).To(BeNil())

	})

})
