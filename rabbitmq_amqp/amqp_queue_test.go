package rabbitmq_amqp

import (
	"context"
	"strconv"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Queue test ", func() {
	var connection *AmqpConnection
	var management *AmqpManagement
	BeforeEach(func() {
		conn, err := Dial(context.TODO(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		connection = conn
		management = connection.Management()

	})

	AfterEach(func() {
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP Queue Declare With Response and Get/Delete should succeed", func() {
		var queueName = generateName("AMQP Queue Declare With Response and Delete should succeed")
		queueInfo, err := management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.IsExclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Quorum))

		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())
	})

	It("AMQP Queue Declare With Parameters and Get/Delete should succeed", func() {
		var queueName = generateName("AMQP Queue Declare With Parameters and Delete should succeed")

		queueInfo, err := management.DeclareQueue(context.TODO(), &ClassicQueueSpecification{
			Name:                 queueName,
			MaxLengthBytes:       CapacityGB(1),
			DeadLetterExchange:   "dead-letter-exchange",
			DeadLetterRoutingKey: "dead-letter-routing-key",
			IsAutoDelete:         true,
			IsExclusive:          true,
		})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeTrue())
		Expect(queueInfo.IsExclusive()).To(BeTrue())
		Expect(queueInfo.Type()).To(Equal(Classic))
		Expect(queueInfo.Leader()).To(ContainSubstring("rabbit"))
		Expect(len(queueInfo.Members())).To(BeNumerically(">", 0))

		Expect(queueInfo.Arguments()).To(HaveKeyWithValue("x-dead-letter-exchange", "dead-letter-exchange"))
		Expect(queueInfo.Arguments()).To(HaveKeyWithValue("x-dead-letter-routing-key", "dead-letter-routing-key"))
		Expect(queueInfo.Arguments()).To(HaveKeyWithValue("x-max-length-bytes", int64(1000000000)))

		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())

	})

	It("AMQP Declare Quorum Queue and Get/Delete should succeed", func() {
		var queueName = generateName("AMQP Declare Quorum Queue and Delete should succeed")
		// Quorum queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueInfo, err := management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name: queueName,
		})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.IsExclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Quorum))
		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())

	})

	It("AMQP Declare Stream Queue and Get/Delete should succeed", func() {
		const queueName = "AMQP Declare Stream Queue and Delete should succeed"
		// Stream queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues

		queueInfo, err := management.DeclareQueue(context.TODO(), &ClassicQueueSpecification{
			Name:         queueName,
			IsAutoDelete: false,
			IsExclusive:  false})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.IsExclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Classic))
		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())

	})

	It("AMQP Declare Queue should fail with Precondition fail", func() {
		// The first queue is declared as Classic, and it should succeed
		// The second queue is declared as Quorum, and it should fail since it is already declared as Classic
		//queueName := generateName("AMQP Declare Queue should fail with Precondition fail")
		queueName := "ab"
		_, err := management.DeclareQueue(context.TODO(), &ClassicQueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())

		_, err = management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name: queueName,
		})

		Expect(err).NotTo(BeNil())
		Expect(err).To(Equal(ErrPreconditionFailed))
		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Queue should fail during validation", func() {
		queueName := generateName("AMQP Declare Queue should fail during validation")
		_, err := management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name:           queueName,
			MaxLengthBytes: -1,
		})

		Expect(err).NotTo(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("AMQP Declare Queue should create client name queue", func() {
		queueInfo, err := management.DeclareQueue(context.TODO(), nil)
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(ContainSubstring("client.gen-"))
		err = management.DeleteQueue(context.TODO(), queueInfo.Name())
		Expect(err).To(BeNil())
	})

	It("AMQP Purge Queue should succeed and return the number of messages purged", func() {
		queueName := generateName("AMQP Purge Queue should succeed and return the number of messages purged")
		queueInfo, err := management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		publishMessages(queueName, 10)
		purged, err := management.PurgeQueue(context.TODO(), queueInfo.Name())
		Expect(err).To(BeNil())
		Expect(purged).To(Equal(10))
		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())
	})

	It("AMQP GET on non-existing queue should return ErrDoesNotExist", func() {
		const queueName = "This queue does not exist"
		result, err := management.QueueInfo(context.TODO(), queueName)
		Expect(err).To(Equal(ErrDoesNotExist))
		Expect(result).To(BeNil())
	})
})

func publishMessages(queueName string, count int) {
	conn, err := Dial(context.TODO(), []string{"amqp://guest:guest@localhost"}, nil)
	Expect(err).To(BeNil())

	publisher, err := conn.NewTargetPublisher(context.TODO(), &QueueAddress{Queue: queueName}, "test")
	Expect(err).To(BeNil())
	Expect(publisher).NotTo(BeNil())

	for i := 0; i < count; i++ {
		publishResult, err := publisher.Publish(context.TODO(), amqp.NewMessage([]byte("Message #"+strconv.Itoa(i))))
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
	}
	err = conn.Close(context.TODO())
	Expect(err).To(BeNil())

}
