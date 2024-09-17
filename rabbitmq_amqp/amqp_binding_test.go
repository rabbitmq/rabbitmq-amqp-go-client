package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Bindings test ", func() {
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

	It("AMQP Bindings between Exchange and Queue Should succeed", func() {
		const exchangeName = "Exchange_AMQP Bindings between Exchange and Queue should uccess"
		const queueName = "Queue_AMQP Bindings between Exchange and Queue should succeed"
		exchangeSpec := management.Exchange(exchangeName)
		exchangeInfo, err := exchangeSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.GetName()).To(Equal(exchangeName))

		queueSpec := management.Queue(queueName)
		queueInfo, err := queueSpec.Declare(context.TODO())
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.GetName()).To(Equal(queueName))

		bindingSpec := management.Binding().SourceExchange(exchangeSpec).DestinationQueue(queueSpec).Key("routing-key")
		err = bindingSpec.Bind(context.TODO())
		Expect(err).To(BeNil())
		err = bindingSpec.Unbind(context.TODO())
		Expect(err).To(BeNil())
		err = exchangeSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
		err = queueSpec.Delete(context.TODO())
		Expect(err).To(BeNil())
	})
})
