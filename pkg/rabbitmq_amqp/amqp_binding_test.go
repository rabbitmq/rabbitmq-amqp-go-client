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
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &TopicExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))

		queueInfo, err := management.DeclareQueue(context.TODO(), &QuorumQueueSpecification{
			Name: queueName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.Name()).To(Equal(queueName))
		bindingPath, err := management.Bind(context.TODO(), &ExchangeToQueueBindingSpecification{
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

	It("AMQP Bindings between Exchange and Exchange Should succeed", func() {
		var exchangeName = generateName("Exchange_AMQP Bindings between Exchange and Exchange should succeed")
		var exchangeName2 = generateName("Exchange_AMQP Bindings between Exchange and Exchange should succeed 2")
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &TopicExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))

		exchangeInfo2, err := management.DeclareExchange(context.TODO(), &TopicExchangeSpecification{
			Name: exchangeName2})
		Expect(err).To(BeNil())
		Expect(exchangeInfo2).NotTo(BeNil())
		Expect(exchangeInfo2.Name()).To(Equal(exchangeName2))

		bindingPath, err := management.Bind(context.TODO(), &ExchangeToExchangeBindingSpecification{
			SourceExchange:      exchangeName,
			DestinationExchange: exchangeName2,
		})

		Expect(err).To(BeNil())
		Expect(management.Unbind(context.TODO(), bindingPath)).To(BeNil())
		Expect(management.DeleteExchange(context.TODO(), exchangeName)).To(BeNil())
		Expect(management.DeleteExchange(context.TODO(), exchangeName2)).To(BeNil())
	})

	It("AMQP Bindings should fail if source or destinations are empty", func() {

		_, err := management.Bind(context.TODO(), &ExchangeToExchangeBindingSpecification{
			SourceExchange:      "",
			DestinationExchange: "destination",
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("source and destination names are required"))

		_, err = management.Bind(context.TODO(), &ExchangeToExchangeBindingSpecification{
			SourceExchange:      "source",
			DestinationExchange: "",
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("source and destination names are required"))

		_, err = management.Bind(context.TODO(), &ExchangeToQueueBindingSpecification{
			SourceExchange:   "",
			DestinationQueue: "destination",
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("source and destination names are required"))

		_, err = management.Bind(context.TODO(), &ExchangeToQueueBindingSpecification{
			SourceExchange:   "source",
			DestinationQueue: "",
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("source and destination names are required"))
	})

	It("AMQP Bindings should fail specification is nil", func() {
		_, err := management.Bind(context.TODO(), nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("binding specification cannot be nil"))
	})
})
