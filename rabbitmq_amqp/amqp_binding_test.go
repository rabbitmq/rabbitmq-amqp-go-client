package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Bindings test ", func() {
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

	It("AMQP Bindings between Exchange and Queue Should succeed", func() {
		const exchangeName = "Exchange_AMQP Bindings between Exchange and Queue should uccess"
		const queueName = "Queue_AMQP Bindings between Exchange and Queue should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &ExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))

		queueInfo, err := management.DeclareQueue(context.TODO(), &QueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		bindingPath, err := management.Bind(context.TODO(), &BindingSpecification{
			SourceExchange:   exchangeName,
			DestinationQueue: queueName,
			BindingKey:       "routing-key",
		})
		Expect(err).To(BeNil())
		err = management.Unbind(context.TODO(), bindingPath)
		Expect(err).To(BeNil())
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
		err = management.DeleteQueue(context.TODO(), queueName)
		Expect(err).To(BeNil())
	})
})
