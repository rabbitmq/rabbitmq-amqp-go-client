package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Management tests", func() {

	It("AMQP Management should fail due of context cancelled", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		err := amqpConnection.Open(context.Background(), NewConnectionSettings())
		Expect(err).To(BeNil())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		err = amqpConnection.Management().Open(ctx, amqpConnection)
		Expect(err).NotTo(BeNil())
	})

	It("Request", func() {
		amqpConnection := NewAmqpConnection()
		Expect(amqpConnection).NotTo(BeNil())
		Expect(amqpConnection).To(BeAssignableToTypeOf(&AmqpConnection{}))

		connectionSettings := NewConnectionSettings()
		Expect(connectionSettings).NotTo(BeNil())
		Expect(connectionSettings).To(BeAssignableToTypeOf(&ConnectionSettings{}))
		err := amqpConnection.Open(context.TODO(), connectionSettings)
		Expect(err).To(BeNil())

		management := amqpConnection.Management()
		kv := make(map[string]any)
		kv["durable"] = true
		kv["auto_delete"] = false
		_queueArguments := make(map[string]any)
		_queueArguments["x-queue-type"] = "quorum"
		kv["arguments"] = _queueArguments
		path := "/queues/test"
		result, err := management.Request(context.TODO(), kv, path, "PUT", []int{200})
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(management.Close(context.TODO())).To(BeNil())
	})
})
