package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("AMQP Connection Test", func() {
	It("AMQP SASLTypeAnonymous Connection should succeed", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, &amqp.ConnOptions{
			SASLType: amqp.SASLTypeAnonymous()})
		Expect(err).To(BeNil())
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})

	It("AMQP SASLTypePlain Connection should succeed", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest")})

		Expect(err).To(BeNil())
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})

	It("AMQP Connection should fail due of wrong Port", func() {
		_, err := Dial(context.Background(), []string{"amqp://localhost:1234"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should fail due of wrong Host", func() {
		_, err := Dial(context.Background(), []string{"amqp://wrong_host:5672"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should fail due to context cancellation", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		_, err := Dial(ctx, []string{"amqp://"}, nil)
		Expect(err).NotTo(BeNil())
	})

	It("AMQP Connection should receive events", func() {
		ch := make(chan *StatusChanged, 1)
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		connection.NotifyStatusChange(ch)
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())

		recv := <-ch
		Expect(recv).NotTo(BeNil())
		Expect(recv.From).To(Equal(Open))
		Expect(recv.To).To(Equal(Closed))
	})

	//It("AMQP TLS Connection should success with SASLTypeAnonymous ", func() {
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
