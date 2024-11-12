package rabbitmq_amqp

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Exchange test ", func() {
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

	It("AMQP Exchange Declare with Default and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare and Delete with Default should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &ExchangeSpecification{
			Name: exchangeName,
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.GetName()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with Topic and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare with Topic and Delete should succeed"
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &ExchangeSpecification{
			Name:         exchangeName,
			ExchangeType: ExchangeType{Topic},
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.GetName()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})

	It("AMQP Exchange Declare with FanOut and Delete should succeed", func() {
		const exchangeName = "AMQP Exchange Declare with FanOut and Delete should succeed"
		//exchangeSpec := management.Exchange(exchangeName).ExchangeType(ExchangeType{FanOut})
		exchangeInfo, err := management.DeclareExchange(context.TODO(), &ExchangeSpecification{
			Name:         exchangeName,
			ExchangeType: ExchangeType{FanOut},
		})
		Expect(err).To(BeNil())
		Expect(exchangeInfo).NotTo(BeNil())
		Expect(exchangeInfo.GetName()).To(Equal(exchangeName))
		err = management.DeleteExchange(context.TODO(), exchangeName)
		Expect(err).To(BeNil())
	})
})
