package rabbitmqamqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("AMQP connection Test", func() {
	It("AMQP SASLTypeAnonymous connection should succeed", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous()})
		Expect(err).To(BeNil())
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})

	It("AMQP SASLTypePlain connection should succeed", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest")})

		Expect(err).To(BeNil())
		Expect(connection.Properties()["product"]).To(Equal("RabbitMQ"))

		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})

	It("AMQP connection connect to the one correct uri and fails the others", func() {
		conn, err := Dial(context.Background(), []string{"amqp://localhost:1234", "amqp://nohost:555", "amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(conn.Close(context.Background()))
	})

	It("AMQP connection should fail due of wrong Port", func() {
		_, err := Dial(context.Background(), []string{"amqp://localhost:1234"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should fail due of wrong Host", func() {
		_, err := Dial(context.Background(), []string{"amqp://wrong_host:5672"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should fails with all the wrong uris", func() {
		_, err := Dial(context.Background(), []string{"amqp://localhost:1234", "amqp://nohost:555", "amqp://nono"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should fail due to context cancellation", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		_, err := Dial(ctx, []string{"amqp://"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should receive events", func() {
		ch := make(chan *StateChanged, 1)
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		connection.NotifyStatusChange(ch)
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())

		recv := <-ch
		Expect(recv).NotTo(BeNil())
		Expect(recv.From).To(Equal(&StateOpen{}))
		Expect(recv.To).To(Equal(&StateClosed{}))
	})

	//It("AMQP TLS connection should success with SASLTypeAnonymous ", func() {
	//	amqpConnection := NewAmqpConnection()
	//	Expect(amqpConnection).NotTo(BeNil())
	//	Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))
	//
	//	connectionSettings := NewConnUrlHelper().
	//		UseSsl(true).Port(5671).TlsConfig(&tls.Config{
	//		//ServerName:         "localhost",
	//		InsecureSkipVerify: true,
	//	})
	//	Expect(connectionSettings).NotTo(BeNil())
	//	Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnUrlHelper{}))
	//	err := amqpConnection.Open(context.Background(), connectionSettings)
	//	Expect(err).To(BeNil())
	//})
})
