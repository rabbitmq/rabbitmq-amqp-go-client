package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target Publisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target Publisher")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		dest, _ := QueueAddress(&qName)
		publisher, err := connection.Publisher(context.Background(), dest, "test")
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		Expect(publisher).To(BeAssignableToTypeOf(&Publisher{}))

		err = publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")))

		Expect(err).To(BeNil())
		nMessages, err := connection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(publisher.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
	})
})
