package rabbitmqamqp

import (
	"context"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Management tests", func() {
	It("AMQP Management should fail due to context cancellation", func() {
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		err = connection.Management().Open(ctx, connection)
		Expect(err).NotTo(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP Management should receive events", func() {
		ch := make(chan *StateChanged, 2)
		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery: false,
			},
		})
		Expect(err).To(BeNil())
		connection.NotifyStatusChange(ch)
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
		recv := <-ch
		Expect(recv).NotTo(BeNil())

		Expect(recv.From).To(Equal(&StateOpen{}))
		Expect(recv.To).To(Equal(&StateClosed{}))
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Request", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())

		management := connection.Management()
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
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("GET on non-existing queue returns ErrDoesNotExist", func() {

		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())

		management := connection.Management()
		path := "/queues/i-do-not-exist"
		result, err := management.Request(context.Background(), amqp.Null{}, path, commandGet, []int{responseCode200, responseCode404})
		Expect(err).To(Equal(ErrDoesNotExist))
		Expect(result).To(BeNil())
	})
})
