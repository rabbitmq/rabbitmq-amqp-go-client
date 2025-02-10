package rabbitmq_amqp

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
})
