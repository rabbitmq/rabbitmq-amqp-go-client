package rabbitmqamqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
	"strconv"
	"time"
)

func publishMessagesWithStreamTag(queueName string, filterValue string, count int) {
	conn, err := Dial(context.TODO(), []string{"amqp://guest:guest@localhost"}, nil)
	Expect(err).To(BeNil())

	publisher, err := conn.NewPublisher(context.TODO(), &QueueAddress{Queue: queueName}, "producer_filter_stream")
	Expect(err).To(BeNil())
	Expect(publisher).NotTo(BeNil())

	for i := 0; i < count; i++ {
		body := filterValue + " #" + strconv.Itoa(i)
		msg := NewMessageWithFilter([]byte(body), filterValue)
		publishResult, err := publisher.Publish(context.TODO(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
	}
	err = conn.Close(context.TODO())
	Expect(err).To(BeNil())

}

var _ = Describe("Consumer stream test", func() {

	It("start consuming with different offset types", func() {
		/*
			Test the different offset types for stream consumers
			1. OffsetValue
			2. OffsetFirst
			3. OffsetLast
			4. OffsetNext

			With 10 messages in the queue, the test will create a consumer with different offset types
			the test 1, 2, 4 can be deterministic. The test 3 can't be deterministic (in this test),
			but we can check if there is at least one message, and it is not the first one.
			It is enough to verify the functionality of the offset types.
		*/

		qName := generateName("start consuming with different offset types")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.name).To(Equal(qName))

		publishMessages(qName, 10)

		consumerOffsetValue, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "offset_value_test",
			InitialCredits:   1,
			Offset:           &OffsetValue{Offset: 5},
		})

		Expect(err).To(BeNil())
		Expect(consumerOffsetValue).NotTo(BeNil())
		Expect(consumerOffsetValue).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 5; i++ {
			dc, err := consumerOffsetValue.Receive(context.Background())
			Expect(err).To(BeNil())

			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i+5)))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerFirst, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			Offset: &OffsetFirst{},
		})

		Expect(err).To(BeNil())
		Expect(consumerFirst).NotTo(BeNil())
		Expect(consumerFirst).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumerFirst.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		// the tests Last and Next can't be deterministic
		// but we can check if there is at least one message, and it is not the first one
		consumerLast, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumerLast_test",
			InitialCredits:   10,
			Offset:           &OffsetLast{},
		})

		Expect(err).To(BeNil())
		Expect(consumerLast).NotTo(BeNil())
		Expect(consumerLast).To(BeAssignableToTypeOf(&Consumer{}))
		// it should receive at least one message
		dc, err := consumerLast.Receive(context.Background())
		Expect(err).To(BeNil())
		Expect(dc.Message()).NotTo(BeNil())
		Expect(fmt.Sprintf("%s", dc.Message().GetData())).NotTo(Equal(fmt.Sprintf("Message #%d", 0)))
		Expect(dc.Accept(context.Background())).To(BeNil())

		consumerNext, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumerNext_next",
			InitialCredits:   10,
			Offset:           &OffsetNext{},
		})

		Expect(err).To(BeNil())
		Expect(consumerNext).NotTo(BeNil())
		Expect(consumerNext).To(BeAssignableToTypeOf(&Consumer{}))
		signal := make(chan struct{})
		go func() {
			// it should receive the next message
			dc, err = consumerNext.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal("the next message"))
			Expect(dc.Accept(context.Background())).To(BeNil())
			signal <- struct{}{}
		}()
		publishMessages(qName, 1, "the next message")
		<-signal
		Expect(consumerLast.Close(context.Background())).To(BeNil())
		Expect(consumerOffsetValue.Close(context.Background())).To(BeNil())
		Expect(consumerFirst.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("consumer should restart form the last offset in case of disconnection", func() {
		/*
			Test the consumer should restart form the last offset in case of disconnection
			So we send 10 messages. Consume 5 then kill the connection and the consumer should restart form
			the offset 5 to consume the messages
		*/

		qName := generateName("consumer should restart form the last offset in case of disconnection")
		connection, err := Dial(context.Background(), []string{"amqp://"}, &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: qName,
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery: true,
				// reduced the reconnect interval to speed up the test.
				// don't use low values in production
				BackOffReconnectInterval: 1_001 * time.Millisecond,
				MaxReconnectAttempts:     5,
			},
		})
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.name).To(Equal(qName))
		publishMessages(qName, 10)

		consumer, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer should restart form the last offset in case of disconnection",
			InitialCredits:   5,
			Offset:           &OffsetFirst{},
		})

		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 5; i++ {
			dc, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(qName)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		time.Sleep(1 * time.Second)

		Eventually(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(qName)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		time.Sleep(500 * time.Millisecond)

		// the consumer should restart from the last offset
		for i := 5; i < 10; i++ {
			dc, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
		}

		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})
	It("consumer should filter messages based on x-stream-filter", func() {
		qName := generateName("consumer should filter messages based on x-stream-filter")
		connection, err := Dial(context.Background(), []string{"amqp://"}, nil)
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.name).To(Equal(qName))
		publishMessagesWithStreamTag(qName, "banana", 10)
		publishMessagesWithStreamTag(qName, "apple", 10)
		publishMessagesWithStreamTag(qName, "", 10)

		consumerBanana, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer banana should filter messages based on x-stream-filter",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			Filters:          []string{"banana"},
		})

		Expect(err).To(BeNil())
		Expect(consumerBanana).NotTo(BeNil())
		Expect(consumerBanana).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumerBanana.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("banana #%d", i)))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerApple, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName:      "consumer apple should filter messages based on x-stream-filter",
			InitialCredits:        200,
			Offset:                &OffsetFirst{},
			Filters:               []string{"apple"},
			FilterMatchUnfiltered: true,
		})

		Expect(err).To(BeNil())
		Expect(consumerApple).NotTo(BeNil())
		Expect(consumerApple).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumerApple.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("apple #%d", i)))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerAppleAndBanana, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer apple and banana should filter messages based on x-stream-filter",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			Filters:          []string{"apple", "banana"},
		})

		Expect(err).To(BeNil())
		Expect(consumerAppleAndBanana).NotTo(BeNil())
		Expect(consumerAppleAndBanana).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 20; i++ {
			dc, err := consumerAppleAndBanana.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			if i < 10 {
				Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("banana #%d", i)))
			} else {
				Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("apple #%d", i-10)))
			}
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerAppleMatchUnfiltered, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName:      "consumer apple should filter messages based on x-stream-filter and FilterMatchUnfiltered true",
			InitialCredits:        200,
			Offset:                &OffsetFirst{},
			Filters:               []string{"apple"},
			FilterMatchUnfiltered: true,
		})

		Expect(err).To(BeNil())
		Expect(consumerAppleMatchUnfiltered).NotTo(BeNil())
		Expect(consumerAppleMatchUnfiltered).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 20; i++ {
			dc, err := consumerAppleMatchUnfiltered.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			if i < 10 {
				Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf("apple #%d", i)))

			} else {
				Expect(fmt.Sprintf("%s", dc.Message().GetData())).To(Equal(fmt.Sprintf(" #%d", i-10)))
			}
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		Expect(consumerApple.Close(context.Background())).To(BeNil())
		Expect(consumerBanana.Close(context.Background())).To(BeNil())
		Expect(consumerAppleAndBanana.Close(context.Background())).To(BeNil())
		Expect(consumerAppleMatchUnfiltered.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

})
