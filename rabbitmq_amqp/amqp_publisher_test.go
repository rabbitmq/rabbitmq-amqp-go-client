package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("AMQP publisher ", func() {
	It("Send a message to a queue with a Message Target Publisher", func() {
		qName := generateNameWithDateTime("Send a message to a queue with a Message Target Publisher")
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		err := amqpConnection.Open(context.Background(), NewConnectionSettings())
		Expect(err).To(BeNil())
		publisher, err := amqpConnection.NewIMPublisher(context.Background())
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		Expect(publisher).To(BeAssignableToTypeOf(&MPublisher{}))
		queueInfo, err := amqpConnection.Management().DeclareQueue(context.Background(), &QueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		err = publisher.Publish(context.Background(), amqp.NewMessage([]byte("hello")), NewAddressBuilder().Queue(qName))
		Expect(err).To(BeNil())
		// TODO: Remove this sleep when the confirmation is done
		time.Sleep(500 * time.Millisecond)
		nMessages, err := amqpConnection.Management().PurgeQueue(context.Background(), qName)
		Expect(err).To(BeNil())
		Expect(nMessages).To(Equal(1))
		Expect(amqpConnection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(publisher.Close(context.Background())).To(BeNil())
	})
})
