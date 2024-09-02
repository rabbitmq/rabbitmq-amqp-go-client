package pkg

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Management tests", func() {
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
		err = management.Request(context.TODO(), "id", kv, path, "PUT", []int{200})
		Expect(err).To(BeNil())
	})
})
