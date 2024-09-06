package rabbitmq_amqp

import (
	"context"
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

	It("AMQP Queue Declare With Response and Delete should success ", func() {
		const queueName = "AMQP Queue Declare With Response and Delete should success"
		queueSpec := management.Queue(queueName)
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.Exclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Classic))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Queue Declare With Parameters and Delete should success ", func() {
		const queueName = "AMQP Queue Declare With Parameters and Delete should success"
		queueSpec := management.Queue(queueName).Exclusive(true).AutoDelete(true).QueueType(Classic)
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeTrue())
		Expect(queueInfo.Exclusive()).To(BeTrue())
		Expect(queueInfo.Type()).To(Equal(Classic))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Quorum Queue and Delete should success ", func() {
		const queueName = "AMQP Declare Quorum Queue and Delete should success"
		// Quorum queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueSpec := management.Queue(queueName).
			Exclusive(true).
			AutoDelete(true).QueueType(Quorum)
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.Exclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Quorum))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})

	It("AMQP Declare Stream Queue and Delete should success ", func() {
		const queueName = "AMQP Declare Stream Queue and Delete should success"
		// Stream queue will ignore Exclusive and AutoDelete settings
		// since they are not supported by quorum queues
		queueSpec := management.Queue(queueName).
			Exclusive(true).
			AutoDelete(true).QueueType(Stream)
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))
		Expect(queueInfo.IsDurable()).To(BeTrue())
		Expect(queueInfo.IsAutoDelete()).To(BeFalse())
		Expect(queueInfo.Exclusive()).To(BeFalse())
		Expect(queueInfo.Type()).To(Equal(Stream))
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})
})
