package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Management tests", func() {
	It("AMQP Management should fail due to context cancellation", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		err := amqpConnection.Open(context.Background(), NewConnectionSettings())
		Expect(err).To(BeNil())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		err = amqpConnection.Management().Open(ctx, amqpConnection)
		Expect(err).NotTo(BeNil())
		amqpConnection.Close(context.Background())
	})

	It("AMQP Management should receive events", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		ch := make(chan *StatusChanged, 1)
		amqpConnection.Management().NotifyStatusChange(ch)
		err := amqpConnection.Open(context.Background(), NewConnectionSettings())
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
		amqpConnection.Close(context.Background())
	})

	It("Request", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))

		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		err := amqpConnection.Open(context.Background(), connectionSettings)
		Expect(err).To(BeNil())

		management := amqpConnection.Management()
		kv := make(map[string]any)
		kv["durable"] = true
		kv["auto_delete"] = false
		_queueArguments := make(map[string]any)
		_queueArguments["x-queue-type"] = "quorum"
		kv["arguments"] = _queueArguments
		path := "/queues/test"
		result, err := management.Request(context.Background(), kv, path, "PUT", []int{200})
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(management.Close(context.Background())).To(BeNil())
		amqpConnection.Close(context.Background())
	})
})
