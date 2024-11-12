package rabbitmq_amqp

import (
	"context"
	"strconv"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Queue test ", func() {
	var connection IConnection
	var management IManagement
	BeforeEach(func() {
		connection = NewAmqpConnection()
		Expect(connection).NotTo(BeNil())
		Expect(connection).To(BeAssignableToTypeOf(&AmqpConnection{}))
		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		err := connection.Open(context.TODO(), connectionSettings)
		Expect(err).To(BeNil())
		management = connection.Management()

	})

	AfterEach(func() {
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP Queue Declare With Response and Get/Delete should succeed", func() {
		const queueName = "AMQP Queue Declare With Response and Delete should succeed"
		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
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

	It("AMQP Queue Declare With Parameters and Get/Delete should succeed", func() {
		const queueName = "AMQP Queue Declare With Parameters and Delete should succeed"

		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:                 queueName,
			IsAutoDelete:         true,
			IsExclusive:          true,
			QueueType:            QueueType{Classic},
			MaxLengthBytes:       CapacityGB(1),
			DeadLetterExchange:   "dead-letter-exchange",
			DeadLetterRoutingKey: "dead-letter-routing-key",
		})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeTrue())
		Expect(queueInfo.IsExclusive()).To(BeTrue())
		Expect(queueInfo.Type()).To(Equal(Classic))
		Expect(queueInfo.GetLeader()).To(ContainSubstring("rabbit"))
		Expect(len(queueInfo.GetReplicas())).To(BeNumerically(">", 0))

		Expect(queueInfo.GetArguments()).To(HaveKeyWithValue("x-dead-letter-exchange", "dead-letter-exchange"))
		Expect(queueInfo.GetArguments()).To(HaveKeyWithValue("x-dead-letter-routing-key", "dead-letter-routing-key"))
		Expect(queueInfo.GetArguments()).To(HaveKeyWithValue("max-length-bytes", int64(1000000000)))

		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())

	})

	It("AMQP Declare Quorum Queue and Get/Delete should succeed", func() {
		const queueName = "AMQP Declare Quorum Queue and Delete should succeed"
		// Quorum queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:         queueName,
			IsAutoDelete: true,
			IsExclusive:  true,
			QueueType:    QueueType{Quorum},
		})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
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

		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:         queueName,
			IsAutoDelete: true,
			IsExclusive:  true,
			QueueType:    QueueType{Stream},
		})

		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.IsExclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Stream))
		// validate GET (query queue info)
		queueInfoReceived, err := management.QueueInfo(context.TODO(), queueName)
		Expect(queueInfoReceived).To(Equal(queueInfo))

		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())

	})

	It("AMQP Declare Queue with invalid type should fail", func() {
		const queueName = "AMQP Declare Queue with invalid type should fail"
		_, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:      queueName,
			QueueType: QueueType{Type: "invalid"},
		})
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Declare Queue should fail with Precondition fail", func() {
		// The first queue is declared as Classic, and it should succeed
		// The second queue is declared as Quorum, and it should fail since it is already declared as Classic
		const queueName = "AMQP Declare Queue should fail with Precondition fail"

		_, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:      queueName,
			QueueType: QueueType{Classic},
		})
		Expect(err).To(BeNil())

		_, err = management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name:      queueName,
			QueueType: QueueType{Quorum},
		})

		Expect(err).NotTo(BeNil())
		Expect(err).To(Equal(ErrPreconditionFailed))
		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Queue should fail during validation", func() {
		const queueName = "AMQP Declare Queue should fail during validation"
		_, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
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
		Expect(queueInfo.GetName()).To(ContainSubstring("client.gen-"))
		err = management.DeleteQueue(context.TODO(), queueInfo.GetName())
		Expect(err).To(BeNil())
	})

	It("AMQP Purge Queue should succeed and return the number of messages purged", func() {
		const queueName = "AMQP Purge Queue should succeed and return the number of messages purged"
		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		publishMessages(queueName, 10)
		purged, err := management.PurgeQueue(context.TODO(), queueInfo.GetName())
		Expect(err).To(BeNil())
		Expect(purged).To(Equal(10))
	})

	It("AMQP GET on non-existing queue should return ErrDoesNotExist", func() {
		const queueName = "This queue does not exist"
		result, err := management.QueueInfo(context.TODO(), queueName)
		Expect(err).To(Equal(ErrDoesNotExist))
		Expect(result).To(BeNil())
	})
})

// TODO: This should be replaced with this library's publish function
// but for the time being, we need a way to publish messages or test purposes
func publishMessages(queueName string, count int) {
	conn, err := amqp.Dial(context.TODO(), "amqp://guest:guest@localhost", nil)
	if err != nil {
		Fail(err.Error())
	}
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		Fail(err.Error())
	}
	sender, err := session.NewSender(context.TODO(), queuePath(queueName), nil)
	if err != nil {
		Fail(err.Error())
	}

	for i := 0; i < count; i++ {
		err = sender.Send(context.TODO(), amqp.NewMessage([]byte("Message #"+strconv.Itoa(i))), nil)
		if err != nil {
			Fail(err.Error())
		}
	}

}
