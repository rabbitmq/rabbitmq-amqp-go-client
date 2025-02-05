package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	test_helper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
	"time"
)

var _ = Describe("Recovery connection test", func() {
	It("connection should reconnect if dropped by via REST API", func() {
		containerID := "connection should reconnect if dropped by via REST API"
		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: containerID,
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 2 * time.Second,
				MaxReconnectAttempts:     5,
			},
		})
		Expect(err).To(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)

		Eventually(func() bool {
			err := test_helper.DropConnectionContainerID(containerID)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		<-ch
		time.Sleep(2 * time.Second)
		Eventually(func() bool {
			conn, err := test_helper.GetConnectionByContainerID(containerID)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		<-ch
		err = connection.Close(context.Background())
		<-ch
		Expect(err).To(BeNil())
	})
})
