package pkg

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

	It("AMQP Queue should success with default creation", func() {
		const queueName = "AMQPQueueShouldSuccessWithDefaultCreation"
		queueSpec := management.Queue(queueName)
		err, i := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(i).NotTo(BeNil())
	})
})