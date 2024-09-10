package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("AMQP Connection Test", func() {
	It("AMQP Connection should success", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))

		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		err := amqpConnection.Open(context.TODO(), connectionSettings)
		Expect(err).To(BeNil())
	})

	It("AMQP Connection should fail due of wrong port", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))
		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		connectionSettings.Host("localhost").Port(1234)
		err := amqpConnection.Open(context.TODO(), connectionSettings)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should fail due of wrong host", func() {

		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))

		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		connectionSettings.Host("wronghost").Port(5672)
		err := amqpConnection.Open(context.TODO(), connectionSettings)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should fail due of context cancelled", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		err := amqpConnection.Open(ctx, NewConnectionSettings())
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should receive events ", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		ch := make(chan *StatusChanged, 1)
		amqpConnection.NotifyStatusChange(ch)
		err := amqpConnection.Open(context.TODO(), NewConnectionSettings())
		Expect(err).To(BeNil())
		recv := <-ch
		Expect(recv).NotTo(BeNil())
		Expect(recv.From).To(Equal(Closed))
		Expect(recv.To).To(Equal(Open))

		err = amqpConnection.Close(context.Background())
		Expect(err).To(BeNil())
		recv = <-ch
		Expect(recv).NotTo(BeNil())

		Expect(recv.From).To(Equal(Open))
		Expect(recv.To).To(Equal(Closed))

	})

	//It("AMQP TLS Connection should success with SASLTypeAnonymous ", func() {
	//	amqpConnection := NewAmqpConnection()
	//	Expect(amqpConnection).NotTo(BeNil())
	//	Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))
	//
	//	connectionSettings := NewConnectionSettings().
	//		UseSsl(true).Port(5671).TlsConfig(&tls.Config{
	//		//ServerName:         "localhost",
	//		InsecureSkipVerify: true,
	//	})
	//	Expect(connectionSettings).NotTo(BeNil())
	//	Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
	//	err := amqpConnection.Open(context.TODO(), connectionSettings)
	//	Expect(err).To(BeNil())
	//})

})
