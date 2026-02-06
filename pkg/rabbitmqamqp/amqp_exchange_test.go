package rabbitmqamqp

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Exchange test ", func() {
	var connection *AmqpConnection
	var management *AmqpManagement
	BeforeEach(func() {
		conn, err := Dial(context.TODO(), "amqp://", nil)
		connection = conn
		Expect(err).To(BeNil())
		management = connection.Management()
	})

	AfterEach(func() {
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP Exchange Declare with Default and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare and Delete with DefaultSettle should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &DirectExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with Topic and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare with Topic and Delete should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &TopicExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with FanOut and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare with FanOut and Delete should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &FanOutExchangeSpecification{
			Name: exchangeName,
			Arguments: map[string]any{
				"x-foo": "bar",
			},
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with Custom Exchange and Delete should succeed", func() {
		var exchangeName = generateName("AMQP Exchange Declare with Custom Exchange and Delete should succeed")

		exchangeInfo, err := management.DeclareExchange(context.TODO(), &CustomExchangeSpecification{
			Name:             exchangeName,
			ExchangeTypeName: "x-local-random",
			Arguments: map[string]any{
				"x-delayed-type": "direct",
			},
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with Headers Exchange and Delete should succeed", func() {
		var exchangeName = generateName("AMQP Exchange Declare with Headers Exchange and Delete should succeed")

		exchangeInfo, err := management.DeclareExchange(context.TODO(), &HeadersExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.Name()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange should fail when specification is nil", func() {
		_, err := management.DeclareExchange(context.TODO(), nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("exchange specification cannot be nil"))
	})

	It("AMQP Exchange should fail when name is empty", func() {
		_, err := management.DeclareExchange(context.TODO(), &TopicExchangeSpecification{
			Name:         "",
			IsAutoDelete: false,
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("exchange name cannot be empty"))
	})
})
