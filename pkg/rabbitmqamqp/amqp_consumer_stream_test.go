package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
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
		connection, err := Dial(context.Background(), "amqp://", nil)
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

			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i+5)))
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
			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
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
		Expect(string(dc.Message().GetData())).NotTo(Equal(fmt.Sprintf("Message #%d", 0)))
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
			Expect(string(dc.Message().GetData())).To(Equal("the next message"))
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
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
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
			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
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
			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message #%d", i)))
		}

		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})
	It("consumer should filter messages based on x-stream-filter", func() {
		qName := generateName("consumer should filter messages based on x-stream-filter")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())
		Expect(queueInfo.name).To(Equal(qName))
		publishMessagesWithMessageLogic(qName, "banana", 10, func(msg *amqp.Message) {
			msg.Annotations = amqp.Annotations{
				// here we set the filter value taken from the filters array
				StreamFilterValue: "banana",
			}

		})
		publishMessagesWithMessageLogic(qName, "apple", 10, func(msg *amqp.Message) {
			msg.Annotations = amqp.Annotations{
				// here we set the filter value taken from the filters array
				StreamFilterValue: "apple",
			}
		})

		publishMessagesWithMessageLogic(qName, "", 10, func(msg *amqp.Message) {
			msg.Annotations = amqp.Annotations{
				// here we set the filter value taken from the filters array
				StreamFilterValue: "",
			}
		})

		consumerBanana, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer banana should filter messages based on x-stream-filter",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			StreamFilterOptions: &StreamFilterOptions{
				Values: []string{"banana"},
			},
		})

		Expect(err).To(BeNil())
		Expect(consumerBanana).NotTo(BeNil())
		Expect(consumerBanana).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumerBanana.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, "banana")))
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerApple, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer apple should filter messages based on x-stream-filter",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			StreamFilterOptions: &StreamFilterOptions{
				Values:          []string{"apple"},
				MatchUnfiltered: true,
			},
		})

		Expect(err).To(BeNil())
		Expect(consumerApple).NotTo(BeNil())
		Expect(consumerApple).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 10; i++ {
			dc, err := consumerApple.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, "apple")))

			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerAppleAndBanana, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer apple and banana should filter messages based on x-stream-filter",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			StreamFilterOptions: &StreamFilterOptions{
				Values: []string{"apple", "banana"},
			},
		})

		Expect(err).To(BeNil())
		Expect(consumerAppleAndBanana).NotTo(BeNil())
		Expect(consumerAppleAndBanana).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 20; i++ {
			dc, err := consumerAppleAndBanana.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			if i < 10 {
				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, "banana")))
			} else {

				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i-10, "apple")))

			}
			Expect(dc.Accept(context.Background())).To(BeNil())
		}

		consumerAppleMatchUnfiltered, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
			ReceiverLinkName: "consumer apple should filter messages based on x-stream-filter and MatchUnfiltered true",
			InitialCredits:   200,
			Offset:           &OffsetFirst{},
			StreamFilterOptions: &StreamFilterOptions{
				Values:          []string{"apple"},
				MatchUnfiltered: true,
			},
		})

		Expect(err).To(BeNil())
		Expect(consumerAppleMatchUnfiltered).NotTo(BeNil())
		Expect(consumerAppleMatchUnfiltered).To(BeAssignableToTypeOf(&Consumer{}))
		for i := 0; i < 20; i++ {
			dc, err := consumerAppleMatchUnfiltered.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(dc.Message()).NotTo(BeNil())
			if i < 10 {
				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, "apple")))
			} else {
				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i-10, "")))
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

	Describe("consumer should filter messages based on application properties", func() {
		qName := generateName("consumer should filter messages based on application properties")
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())

		publishMessagesWithMessageLogic(qName, "ignoredKey", 7, func(msg *amqp.Message) {
			msg.ApplicationProperties = map[string]interface{}{"ignoredKey": "ignoredValue"}
		})

		publishMessagesWithMessageLogic(qName, "key1", 10, func(msg *amqp.Message) {
			msg.ApplicationProperties = map[string]interface{}{"key1": "value1", "constFilterKey": "constFilterValue"}
		})

		publishMessagesWithMessageLogic(qName, "key2", 10, func(msg *amqp.Message) {
			msg.ApplicationProperties = map[string]interface{}{"key2": "value2", "constFilterKey": "constFilterValue"}
		})

		publishMessagesWithMessageLogic(qName, "key3", 10, func(msg *amqp.Message) {
			msg.ApplicationProperties = map[string]interface{}{"key3": "value3", "constFilterKey": "constFilterValue"}
		})

		var wg sync.WaitGroup
		wg.Add(3)
		DescribeTable("consumer should filter messages based on application properties", func(key string, value any, label string) {

			consumer, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
				InitialCredits: 200,
				Offset:         &OffsetFirst{},
				StreamFilterOptions: &StreamFilterOptions{
					ApplicationProperties: map[string]any{
						key: value,
						// this is a constant filter append during the publishMessagesWithApplicationProperties
						// to test the multiple filters
						"constFilterKey": "constFilterValue",
					},
				},
			})

			Expect(err).To(BeNil())
			Expect(consumer).NotTo(BeNil())
			Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
			for i := 0; i < 10; i++ {
				dc, err := consumer.Receive(context.Background())
				Expect(err).To(BeNil())
				Expect(dc.Message()).NotTo(BeNil())
				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, label)))

				Expect(dc.message.ApplicationProperties).To(HaveKeyWithValue(key, value))
				Expect(dc.Accept(context.Background())).To(BeNil())
			}
			Expect(consumer.Close(context.Background())).To(BeNil())
			wg.Done()
		},
			Entry("key1 value1", "key1", "value1", "key1"),
			Entry("key2 value2", "key2", "value2", "key2"),
			Entry("key3 value3", "key3", "value3", "key3"),
		)
		go func() {
			wg.Wait()
			Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
			Expect(connection.Close(context.Background())).To(BeNil())
		}()

	})

	Describe("consumer should filter messages based on properties", func() {
		/*
			Test the consumer should filter messages based on properties
		*/
		// TODO: defer cleanup to delete the stream queue
		qName := generateName("consumer should filter messages based on properties")
		qName += time.Now().String()
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &StreamQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())

		publishMessagesWithMessageLogic(qName, "MessageID", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{MessageID: "MessageID"}
		})

		publishMessagesWithMessageLogic(qName, "Subject", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{Subject: ptr("Subject")}
		})

		publishMessagesWithMessageLogic(qName, "ReplyTo", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{ReplyTo: ptr("ReplyTo")}
		})

		publishMessagesWithMessageLogic(qName, "ContentType", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{ContentType: ptr("ContentType")}
		})

		publishMessagesWithMessageLogic(qName, "ContentEncoding", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{ContentEncoding: ptr("ContentEncoding")}
		})

		publishMessagesWithMessageLogic(qName, "GroupID", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{GroupID: ptr("GroupID")}
		})

		publishMessagesWithMessageLogic(qName, "ReplyToGroupID", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{ReplyToGroupID: ptr("ReplyToGroupID")}
		})

		// GroupSequence
		publishMessagesWithMessageLogic(qName, "GroupSequence", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{GroupSequence: ptr(uint32(137))}
		})

		// ReplyToGroupID
		publishMessagesWithMessageLogic(qName, "ReplyToGroupID", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{ReplyToGroupID: ptr("ReplyToGroupID")}
		})

		// CreationTime

		publishMessagesWithMessageLogic(qName, "CreationTime", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{CreationTime: ptr(createDateTime())}
		})

		// AbsoluteExpiryTime

		publishMessagesWithMessageLogic(qName, "AbsoluteExpiryTime", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{AbsoluteExpiryTime: ptr(createDateTime())}
		})

		// CorrelationID

		publishMessagesWithMessageLogic(qName, "CorrelationID", 10, func(msg *amqp.Message) {
			msg.Properties = &amqp.MessageProperties{CorrelationID: "CorrelationID"}
		})

		var wg sync.WaitGroup
		wg.Add(12)
		DescribeTable("consumer should filter messages based on properties", func(properties *amqp.MessageProperties, label string) {

			consumer, err := connection.NewConsumer(context.Background(), qName, &StreamConsumerOptions{
				InitialCredits: 200,
				Offset:         &OffsetFirst{},
				StreamFilterOptions: &StreamFilterOptions{
					Properties: properties,
				},
			})

			Expect(err).To(BeNil())
			Expect(consumer).NotTo(BeNil())
			Expect(consumer).To(BeAssignableToTypeOf(&Consumer{}))
			for i := 0; i < 10; i++ {
				dc, err := consumer.Receive(context.Background())
				Expect(err).To(BeNil())
				Expect(dc.Message()).NotTo(BeNil())
				Expect(string(dc.Message().GetData())).To(Equal(fmt.Sprintf("Message_id:%d_label:%s", i, label)))
				// we test one by one because of the date time fields
				// It is not possible to compare the whole structure due of the time
				// It is not perfect but it is enough for the test
				if dc.message.Properties.MessageID != nil {
					Expect(dc.message.Properties.MessageID).To(Equal(properties.MessageID))
				}
				if dc.message.Properties.Subject != nil {
					Expect(dc.message.Properties.Subject).To(Equal(properties.Subject))
				}
				if dc.message.Properties.ReplyTo != nil {
					Expect(dc.message.Properties.ReplyTo).To(Equal(properties.ReplyTo))
				}
				if dc.message.Properties.ContentType != nil {
					Expect(dc.message.Properties.ContentType).To(Equal(properties.ContentType))
				}
				if dc.message.Properties.ContentEncoding != nil {
					Expect(dc.message.Properties.ContentEncoding).To(Equal(properties.ContentEncoding))
				}
				if dc.message.Properties.GroupID != nil {
					Expect(dc.message.Properties.GroupID).To(Equal(properties.GroupID))
				}
				if dc.message.Properties.ReplyToGroupID != nil {
					Expect(dc.message.Properties.ReplyToGroupID).To(Equal(properties.ReplyToGroupID))
				}
				if dc.message.Properties.GroupSequence != nil {
					Expect(dc.message.Properties.GroupSequence).To(Equal(properties.GroupSequence))
				}

				if dc.message.Properties.ReplyToGroupID != nil {
					Expect(dc.message.Properties.ReplyToGroupID).To(Equal(properties.ReplyToGroupID))
				}

				// here we compare only the year, month and day
				// it is not perfect but it is enough for the test
				if dc.message.Properties.CreationTime != nil {
					Expect(dc.message.Properties.CreationTime.Year()).To(Equal(properties.CreationTime.Year()))
					Expect(dc.message.Properties.CreationTime.Month()).To(Equal(properties.CreationTime.Month()))
					Expect(dc.message.Properties.CreationTime.Day()).To(Equal(properties.CreationTime.Day()))
				}

				if dc.message.Properties.AbsoluteExpiryTime != nil {
					Expect(dc.message.Properties.AbsoluteExpiryTime.Year()).To(Equal(properties.AbsoluteExpiryTime.Year()))
					Expect(dc.message.Properties.AbsoluteExpiryTime.Month()).To(Equal(properties.AbsoluteExpiryTime.Month()))
					Expect(dc.message.Properties.AbsoluteExpiryTime.Day()).To(Equal(properties.AbsoluteExpiryTime.Day()))
				}

				if dc.message.Properties.CorrelationID != nil {
					Expect(dc.message.Properties.CorrelationID).To(Equal(properties.CorrelationID))
				}

				Expect(dc.Accept(context.Background())).To(BeNil())
			}
			Expect(consumer.Close(context.Background())).To(BeNil())
			wg.Done()
		},
			Entry("MessageID", &amqp.MessageProperties{MessageID: "MessageID"}, "MessageID"),
			Entry("Subject", &amqp.MessageProperties{Subject: ptr("Subject")}, "Subject"),
			Entry("ReplyTo", &amqp.MessageProperties{ReplyTo: ptr("ReplyTo")}, "ReplyTo"),
			Entry("ContentType", &amqp.MessageProperties{ContentType: ptr("ContentType")}, "ContentType"),
			Entry("ContentEncoding", &amqp.MessageProperties{ContentEncoding: ptr("ContentEncoding")}, "ContentEncoding"),
			Entry("GroupID", &amqp.MessageProperties{GroupID: ptr("GroupID")}, "GroupID"),
			Entry("ReplyToGroupID", &amqp.MessageProperties{ReplyToGroupID: ptr("ReplyToGroupID")}, "ReplyToGroupID"),
			Entry("GroupSequence", &amqp.MessageProperties{GroupSequence: ptr(uint32(137))}, "GroupSequence"),
			Entry("ReplyToGroupID", &amqp.MessageProperties{ReplyToGroupID: ptr("ReplyToGroupID")}, "ReplyToGroupID"),
			Entry("CreationTime", &amqp.MessageProperties{CreationTime: ptr(createDateTime())}, "CreationTime"),
			Entry("AbsoluteExpiryTime", &amqp.MessageProperties{AbsoluteExpiryTime: ptr(createDateTime())}, "AbsoluteExpiryTime"),
			Entry("CorrelationID", &amqp.MessageProperties{CorrelationID: "CorrelationID"}, "CorrelationID"),
		)
		go func() {
			wg.Wait()
			Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
			Expect(connection.Close(context.Background())).To(BeNil())
		}()
	})

})

type msgLogic = func(*amqp.Message)

func publishMessagesWithMessageLogic(queue string, label string, count int, logic msgLogic) {
	conn, err := Dial(context.TODO(), "amqp://guest:guest@localhost", nil)
	Expect(err).To(BeNil())

	publisher, err := conn.NewPublisher(context.TODO(), &QueueAddress{Queue: queue},
		nil)
	Expect(err).To(BeNil())
	Expect(publisher).NotTo(BeNil())

	for i := 0; i < count; i++ {
		body := fmt.Sprintf("Message_id:%d_label:%s", i, label)
		msg := NewMessage([]byte(body))
		logic(msg)
		publishResult, err := publisher.Publish(context.TODO(), msg)
		Expect(err).To(BeNil())
		Expect(publishResult).NotTo(BeNil())
		Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
	}
	err = conn.Close(context.TODO())
	Expect(err).To(BeNil())
}
