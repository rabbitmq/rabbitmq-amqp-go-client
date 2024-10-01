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
		queueSpec := management.Queue(queueName)
		queueInfo, err := queueSpec.Declare(context.TODO())
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

		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Queue Declare With Parameters and Get/Delete should succeed", func() {
		const queueName = "AMQP Queue Declare With Parameters and Delete should succeed"
		queueSpec := management.Queue(queueName).Exclusive(true).
			AutoDelete(true).
			QueueType(QueueType{Classic}).
			MaxLengthBytes(CapacityGB(1)).
			DeadLetterExchange("dead-letter-exchange").
			DeadLetterRoutingKey("dead-letter-routing-key")
		queueInfo, err := queueSpec.Declare(context.TODO())
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

		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Quorum Queue and Get/Delete should succeed", func() {
		const queueName = "AMQP Declare Quorum Queue and Delete should succeed"
		// Quorum queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueSpec := management.Queue(queueName).
			Exclusive(true).
			AutoDelete(true).QueueType(QueueType{Quorum})
		queueInfo, err := queueSpec.Declare(context.TODO())
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

		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Stream Queue and Get/Delete should succeed", func() {
		const queueName = "AMQP Declare Stream Queue and Delete should succeed"
		// Stream queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueSpec := management.Queue(queueName).
			Exclusive(true).
			AutoDelete(true).QueueType(QueueType{Stream})
		queueInfo, err := queueSpec.Declare(context.TODO())
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

		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Queue with invalid type should fail", func() {
		const queueName = "AMQP Declare Queue with invalid type should fail"
		queueSpec := management.Queue(queueName).
			QueueType(QueueType{Type: "invalid"})
		_, err := queueSpec.Declare(context.TODO())
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Declare Queue should fail with Precondition fail", func() {
		// The first queue is declared as Classic and it should succeed
		// The second queue is declared as Quorum and it should fail since it is already declared as Classic
		const queueName = "AMQP Declare Queue should fail with Precondition fail"
		queueSpec := management.Queue(queueName).QueueType(QueueType{Classic})
		_, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		queueSpecFail := management.Queue(queueName).QueueType(QueueType{Quorum})
		_, err = queueSpecFail.Declare(context.TODO())
		Expect(err).NotTo(BeNil())
		Expect(err).To(Equal(ErrPreconditionFailed))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Queue should fail during validation", func() {
		const queueName = "AMQP Declare Queue should fail during validation"
		queueSpec := management.Queue(queueName).MaxLengthBytes(-1)
		_, err := queueSpec.Declare(context.TODO())
		Expect(err).NotTo(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("AMQP Declare Queue should create client name queue", func() {
		queueSpec := management.QueueClientName()
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(ContainSubstring("client.gen-"))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Purge Queue should succeed and return the number of messages purged", func() {
		const queueName = "AMQP Purge Queue should succeed and return the number of messages purged"
		queueSpec := management.Queue(queueName)
		_, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		publishMessages(queueName, 10)
		purged, err := queueSpec.Purge(context.TODO())
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
