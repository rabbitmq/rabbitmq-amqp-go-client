package rabbitmqamqp

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
)

var _ = Describe("Recovery connection test", func() {
	It("connection should reconnect producers and consumers if dropped by via REST API", func() {
		/*
			The test is a bit complex since it requires to drop the connection by REST API
			Then wait for the connection to be reconnected.
			The scope of the test is to verify that the connection is reconnected and the
			producers and consumers are able to send and receive messages.
			It is more like an integration test.
			This kind of the tests requires time in terms of execution it has to wait for the
			connection to be reconnected, so to speed up the test I aggregated the tests in one.
		*/

		name := "connection should reconnect producers and consumers if dropped by via REST API"
		env := NewEnvironment("amqp://", &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: name,
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 2 * time.Second,
				MaxReconnectAttempts:     5,
			},
			Id: "reconnect producers and consumers",
		})

		connection, err := env.NewConnection(context.Background())
		Expect(err).To(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)

		qName := generateName(name)
		queueInfo, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(queueInfo).NotTo(BeNil())

		consumer, err := connection.NewConsumer(context.Background(),
			qName, nil)
		Expect(err).To(BeNil())

		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{
			Queue: qName,
		}, nil)

		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		for i := 0; i < 5; i++ {
			publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello")))
			Expect(err).To(BeNil())
			Expect(publishResult).NotTo(BeNil())
			Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		}

		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(name)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st1 := <-ch
		Expect(st1.From).To(Equal(&StateOpen{}))
		Expect(st1.To).To(BeAssignableToTypeOf(&StateClosed{}))
		///  Closed state should have an error
		// Since it is forced closed by the REST API
		err = st1.To.(*StateClosed).GetError()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("Connection forced"))

		time.Sleep(1 * time.Second)
		Eventually(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(name)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st2 := <-ch
		Expect(st2.From).To(BeAssignableToTypeOf(&StateClosed{}))
		Expect(st2.To).To(Equal(&StateReconnecting{}))

		st3 := <-ch
		Expect(st3.From).To(BeAssignableToTypeOf(&StateReconnecting{}))
		Expect(st3.To).To(Equal(&StateOpen{}))

		for i := 0; i < 5; i++ {
			publishResult, err := publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello")))
			Expect(err).To(BeNil())
			Expect(publishResult).NotTo(BeNil())
			Expect(publishResult.Outcome).To(Equal(&amqp.StateAccepted{}))
		}

		/// after the connection is reconnected the consumer should be able to receive the messages
		for i := 0; i < 10; i++ {
			deliveryContext, err := consumer.Receive(context.Background())
			Expect(err).To(BeNil())
			Expect(deliveryContext).NotTo(BeNil())
		}

		Expect(connection.Management().DeleteQueue(context.Background(), qName)).To(BeNil())

		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
		st4 := <-ch
		Expect(st4.From).To(Equal(&StateOpen{}))
		Expect(st4.To).To(BeAssignableToTypeOf(&StateClosed{}))
		err = st4.To.(*StateClosed).GetError()
		// the flow status should be:
		// from open to closed (with error)
		// from closed to reconnecting
		// from reconnecting to open
		// from open to closed (without error)
		Expect(err).To(BeNil())

		Expect(consumer.Close(context.Background())).NotTo(BeNil())
		Expect(publisher.Close(context.Background())).NotTo(BeNil())

		entLen := 0
		connection.entitiesTracker.consumers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(0))

		entLen = 0
		connection.entitiesTracker.publishers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(0))
	})

	It("connection should not reconnect producers and consumers if the auto-recovery is disabled", func() {
		name := "connection should reconnect producers and consumers if dropped by via REST API"
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType:    amqp.SASLTypeAnonymous(),
			ContainerID: name,
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery: false, // disabled
			},
		})
		Expect(err).To(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)

		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(name)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st1 := <-ch
		Expect(st1.From).To(Equal(&StateOpen{}))
		Expect(st1.To).To(BeAssignableToTypeOf(&StateClosed{}))

		err = st1.To.(*StateClosed).GetError()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("Connection forced"))

		time.Sleep(1 * time.Second)

		// the connection should not be reconnected
		Consistently(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(name)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeFalse())

		err = connection.Close(context.Background())
		Expect(err).NotTo(BeNil())
	})

	It("validate the Recovery connection parameters", func() {

		_, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 500 * time.Millisecond,
				MaxReconnectAttempts:     5,
			},
		})
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("BackOffReconnectInterval should be greater than"))

		_, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:       true,
				MaxReconnectAttempts: 0,
			},
		})

		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("MaxReconnectAttempts should be greater than"))
	})

	Context("topology recovery", func() {
		DescribeTable("Queue record returns the expected queue specification",
			func(queueType TQueueType, autoDelete bool, exclusive bool, arguments map[string]any) {
				queue := &queueRecoveryRecord{
					queueName:  "test-queue",
					queueType:  queueType,
					autoDelete: &autoDelete,
					exclusive:  &exclusive,
					arguments:  arguments,
				}
				queueSpec := queue.toIQueueSpecification()
				Expect(queueSpec).NotTo(BeNil())
				Expect(queueSpec.name()).To(Equal("test-queue"))
				Expect(queueSpec.queueType()).To(Equal(queueType))
				Expect(queueSpec.isAutoDelete()).To(Equal(autoDelete))
				Expect(queueSpec.isExclusive()).To(Equal(exclusive))
				arguments["x-queue-type"] = string(queueType)
				Expect(queueSpec.buildArguments()).To(Equal(arguments))
			},
			Entry("Quorum queue", Quorum, false, false, map[string]any{}),
			Entry("Classic queue", Classic, true, true, map[string]any{}),
			Entry("Stream queue", Stream, false, false, map[string]any{}),
			Entry("Quorum queue with arguments", Quorum, false, false, map[string]any{"x-max-length-bytes": 1000}),
			Entry("Classic queue with arguments", Classic, true, true, map[string]any{"x-max-length-bytes": 1000}),
			Entry("Stream queue with arguments", Stream, false, false, map[string]any{"x-max-length-bytes": 1000}),
		)

		DescribeTable("Exchange record returns the expected exchange specification",
			func(exchangeType TExchangeType, autoDelete bool, arguments map[string]any) {
				exchange := &exchangeRecoveryRecord{
					exchangeName: "test-exchange",
					exchangeType: exchangeType,
					autoDelete:   autoDelete,
					arguments:    arguments,
				}
				exchangeSpec := exchange.toIExchangeSpecification()
				Expect(exchangeSpec).NotTo(BeNil())
				Expect(exchangeSpec.name()).To(Equal("test-exchange"))
				Expect(exchangeSpec.exchangeType()).To(Equal(exchangeType))
				Expect(exchangeSpec.isAutoDelete()).To(Equal(autoDelete))
				Expect(exchangeSpec.arguments()).To(Equal(arguments))
			},
			Entry("Direct exchange", Direct, false, map[string]any{}),
			Entry("Topic exchange", Topic, false, map[string]any{}),
			Entry("FanOut exchange", FanOut, false, map[string]any{}),
			Entry("Headers exchange", Headers, false, map[string]any{}),
			Entry("Custom exchange", TExchangeType("my-exchange-type"), false, map[string]any{}),
			Entry("Direct exchange with arguments", Direct, true, map[string]any{"x-some-arg": "some-value"}),
			Entry("Topic exchange with arguments", Topic, true, map[string]any{"x-some-arg": "some-value"}),
			Entry("FanOut exchange with arguments", FanOut, true, map[string]any{"x-some-arg": "some-value"}),
			Entry("Headers exchange with arguments", Headers, true, map[string]any{"x-some-arg": "some-value"}),
			Entry("Custom exchange with arguments", TExchangeType("my-exchange-type"), true, map[string]any{"x-some-arg": "some-value"}),
		)

		DescribeTable("Binding record returns the expected binding specification",
			func(sourceExchange string, destination string, isDestinationQueue bool, bindingKey string, arguments map[string]any) {
				binding := &bindingRecoveryRecord{
					sourceExchange:     sourceExchange,
					destination:        destination,
					isDestinationQueue: isDestinationQueue,
					bindingKey:         bindingKey,
					arguments:          arguments,
				}
				bindingSpec := binding.toIBindingSpecification()
				Expect(bindingSpec).ToNot(BeNil())
				Expect(bindingSpec.sourceExchange()).To(Equal(sourceExchange))
				Expect(bindingSpec.destination()).To(Equal(destination))
				Expect(bindingSpec.isDestinationQueue()).To(Equal(isDestinationQueue))
				Expect(bindingSpec.bindingKey()).To(Equal(bindingKey))
				Expect(bindingSpec.arguments()).To(Equal(arguments))
			},
			Entry("Binding to queue", "test-exchange", "test-queue", true, "test-binding-key", map[string]any{}),
			Entry("Binding to exchange", "test-exchange", "test-exchange", false, "test-binding-key", map[string]any{}),
			Entry("Binding to queue with arguments", "test-exchange", "test-queue", true, "test-binding-key", map[string]any{"x-some-arg": "some-value"}),
			Entry("Binding to exchange with arguments", "test-exchange", "test-exchange", false, "test-binding-key", map[string]any{"x-some-arg": "some-value"}),
		)

		Context("with TopologyRecoveryAllEnabled", func() {
			It("adds a recovery record after a successful declaration", func() {
				conn, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
				Expect(err).ToNot(HaveOccurred())

				By("adding a queue record")
				qName := generateName("queueRecordAfterDeclare")
				_, err = conn.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
					Name: qName,
				})
				Expect(err).ToNot(HaveOccurred())

				Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(1))
				queueRecord := conn.topologyRecoveryRecords.queues[0]
				Expect(queueRecord).ToNot(BeNil())
				Expect(queueRecord.queueName).To(Equal(qName))
				Expect(queueRecord.queueType).To(Equal(Quorum))
				Expect(queueRecord.autoDelete).To(BeNil())
				Expect(queueRecord.exclusive).To(BeNil())
				Expect(queueRecord.arguments).To(BeNil())

				By("adding an exchange record")
				exchangeName := generateName("exchangeRecordAfterDeclare")
				_, err = conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
					Name: exchangeName,
				})
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() {
					conn.Management().DeleteQueue(context.Background(), qName)
					conn.Management().DeleteExchange(context.Background(), exchangeName)

					conn.Close(context.Background())
				})

				Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(1))
				exchangeRecord := conn.topologyRecoveryRecords.exchanges[0]
				Expect(exchangeRecord).ToNot(BeNil())
				Expect(exchangeRecord.exchangeName).To(Equal(exchangeName))
				Expect(exchangeRecord.exchangeType).To(Equal(Direct))
				Expect(exchangeRecord.autoDelete).To(BeFalse())
				Expect(exchangeRecord.arguments).To(BeNil())

				By("adding a binding record to a queue")
				bindingKey := generateName("bindingRecordAfterDeclare")
				bindingPath, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
					SourceExchange:   exchangeName,
					DestinationQueue: qName,
					BindingKey:       bindingKey,
				})
				Expect(err).ToNot(HaveOccurred())

				Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))
				bindingRecord := conn.topologyRecoveryRecords.bindings[0]
				Expect(bindingRecord).ToNot(BeNil())
				Expect(bindingRecord.sourceExchange).To(Equal(exchangeName))
				Expect(bindingRecord.destination).To(Equal(qName))
				Expect(bindingRecord.isDestinationQueue).To(BeTrue())
				Expect(bindingRecord.bindingKey).To(Equal(bindingKey))
				Expect(bindingRecord.arguments).To(BeNil())
				Expect(bindingRecord.path).To(Equal(bindingPath))

				By("adding a binding record to an exchange")
				bindingKey = generateName("bindingRecordAfterDeclare")
				bindingPath, err = conn.Management().Bind(context.Background(), &ExchangeToExchangeBindingSpecification{
					SourceExchange:      exchangeName,
					DestinationExchange: "amq.direct",
					BindingKey:          bindingKey,
				})
				Expect(err).ToNot(HaveOccurred())

				Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(2))
				bindingRecord = conn.topologyRecoveryRecords.bindings[1]
				Expect(bindingRecord).ToNot(BeNil())
				Expect(bindingRecord.sourceExchange).To(Equal(exchangeName))
				Expect(bindingRecord.destination).To(Equal("amq.direct"))
				Expect(bindingRecord.isDestinationQueue).To(BeFalseBecause("binding was declared with an exchange destination"))
				Expect(bindingRecord.bindingKey).To(Equal(bindingKey))
				Expect(bindingRecord.path).To(Equal(bindingPath))
			})

			When("a queue is deleted", func() {
				var (
					conn *AmqpConnection
				)

				BeforeEach(func() {
					var err error
					conn, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					Expect(conn.Close(context.Background())).To(Succeed())
				})

				It("should remove the queue record", func() {
					q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:         generateName("test-queue"),
						IsAutoDelete: false,
						IsExclusive:  false,
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(1))
					queueRecord := conn.topologyRecoveryRecords.queues[0]
					Expect(queueRecord).ToNot(BeNil())
					Expect(queueRecord.queueName).To(Equal(q.Name()))
					Expect(queueRecord.autoDelete).ToNot(BeNil())
					Expect(*queueRecord.autoDelete).To(BeFalseBecause("queue was declared as auto delete"))
					Expect(queueRecord.exclusive).ToNot(BeNil())
					Expect(*queueRecord.exclusive).To(BeFalseBecause("queue was declared as exclusive"))

					err = conn.Management().DeleteQueue(context.Background(), q.Name())
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.queues).To(BeEmpty())
				})

				It("should remove only the matching queue record", func() {
					queuePrefix := "top-recovery-test-queue"
					for i := range 3 {
						_, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
							Name:         fmt.Sprintf("%s-%d", queuePrefix, i),
							IsAutoDelete: false,
							IsExclusive:  true,
						})
						Expect(err).ToNot(HaveOccurred())
					}
					Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(3))

					err := conn.Management().DeleteQueue(context.Background(), fmt.Sprintf("%s-1", queuePrefix))
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(2))
					Expect(conn.topologyRecoveryRecords.queues[0].queueName).To(Equal(fmt.Sprintf("%s-0", queuePrefix)))
					Expect(conn.topologyRecoveryRecords.queues[0].exclusive).ToNot(BeNil())
					Expect(*conn.topologyRecoveryRecords.queues[0].exclusive).To(BeTrue())
					Expect(conn.topologyRecoveryRecords.queues[1].queueName).To(Equal(fmt.Sprintf("%s-2", queuePrefix)))
					Expect(conn.topologyRecoveryRecords.queues[1].exclusive).ToNot(BeNil())
					Expect(*conn.topologyRecoveryRecords.queues[1].exclusive).To(BeTrue())
				})

				It("should remove related binding records", func() {
					q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:        generateName("test-queue"),
						IsExclusive: true,
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   "amq.direct",
						DestinationQueue: q.Name(),
						BindingKey:       "test-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   "amq.direct",
						DestinationQueue: q.Name(),
						BindingKey:       "another-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   "amq.direct",
						DestinationQueue: q.Name(),
						BindingKey:       "more-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(3))

					err = conn.Management().DeleteQueue(context.Background(), q.Name())
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(BeEmpty())
				})
			})

			When("an exchange is deleted", func() {
				var (
					conn *AmqpConnection
				)

				BeforeEach(func() {
					var err error
					conn, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					Expect(conn.Close(context.Background())).To(Succeed())
				})

				It("should remove the exchange record", func() {
					e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
						Name:         generateName("test-exchange"),
						IsAutoDelete: false,
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(1))
					exchangeRecord := conn.topologyRecoveryRecords.exchanges[0]
					Expect(exchangeRecord).ToNot(BeNil())
					Expect(exchangeRecord.exchangeName).To(Equal(e.Name()))
					Expect(exchangeRecord.autoDelete).To(BeFalseBecause("exchange was declared as not auto delete"))

					err = conn.Management().DeleteExchange(context.Background(), e.Name())
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.exchanges).To(BeEmpty())
				})

				It("should remove only the matching exchange record", func() {
					exchangePrefix := "top-recovery-test-exchange"
					for i := range 3 {
						_, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
							Name:         fmt.Sprintf("%s-%d", exchangePrefix, i),
							IsAutoDelete: false,
						})
						Expect(err).ToNot(HaveOccurred())
					}
					DeferCleanup(func() {
						c, _ := Dial(context.Background(), "amqp://", nil)
						for i := range 3 {
							_ = c.Management().DeleteExchange(context.Background(), fmt.Sprintf("%s-%d", exchangePrefix, i))
						}
						_ = c.Close(context.Background())
					})
					Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(3))

					err := conn.Management().DeleteExchange(context.Background(), fmt.Sprintf("%s-1", exchangePrefix))
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(2))
					Expect(conn.topologyRecoveryRecords.exchanges[0].exchangeName).To(Equal(fmt.Sprintf("%s-0", exchangePrefix)))
					Expect(conn.topologyRecoveryRecords.exchanges[0].autoDelete).To(BeFalseBecause("exchange was declared as not auto delete"))
					Expect(conn.topologyRecoveryRecords.exchanges[1].exchangeName).To(Equal(fmt.Sprintf("%s-2", exchangePrefix)))
					Expect(conn.topologyRecoveryRecords.exchanges[1].autoDelete).To(BeFalseBecause("exchange was declared as not auto delete"))
				})

				It("should remove related binding records", func() {
					e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
						Name: generateName("test-exchange"),
					})
					Expect(err).ToNot(HaveOccurred())
					q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:        generateName("test-queue"),
						IsExclusive: true,
					})
					Expect(err).ToNot(HaveOccurred())
					q2, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:        generateName("keep-me"),
						IsExclusive: true,
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   e.Name(),
						DestinationQueue: q.Name(),
						BindingKey:       "test-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   e.Name(),
						DestinationQueue: q.Name(),
						BindingKey:       "another-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   e.Name(),
						DestinationQueue: q.Name(),
						BindingKey:       "more-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					_, err = conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   "amq.direct",
						DestinationQueue: q2.Name(),
						BindingKey:       "keep-me-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(4))

					err = conn.Management().DeleteExchange(context.Background(), e.Name())
					Expect(err).ToNot(HaveOccurred())
					By("removing related binding records and preserving unrelated binding records")
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))
					Expect(conn.topologyRecoveryRecords.bindings[0].sourceExchange).To(Equal("amq.direct"))
					Expect(conn.topologyRecoveryRecords.bindings[0].destination).To(Equal(q2.Name()))
					Expect(conn.topologyRecoveryRecords.bindings[0].isDestinationQueue).To(BeTrueBecause("binding was declared with a queue destination"))
					Expect(conn.topologyRecoveryRecords.bindings[0].bindingKey).To(Equal("keep-me-binding-key"))
				})
			})

			When("a binding is deleted", func() {
				var (
					conn *AmqpConnection
				)

				BeforeEach(func() {
					var err error
					conn, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
					Expect(err).ToNot(HaveOccurred())
				})

				AfterEach(func() {
					Expect(conn.Close(context.Background())).To(Succeed())
				})

				It("should remove the binding record", func() {
					e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
						Name: generateName("test-exchange-"),
					})
					Expect(err).ToNot(HaveOccurred())
					DeferCleanup(func() {
						c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
						_ = c.Management().DeleteExchange(context.Background(), e.Name())
						_ = c.Close(context.Background())
					})

					q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:        generateName("test-queue-"),
						IsExclusive: true,
					})
					Expect(err).ToNot(HaveOccurred())
					b, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
						SourceExchange:   e.Name(),
						DestinationQueue: q.Name(),
						BindingKey:       "test-binding-key",
					})
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))
					bindingRecord := conn.topologyRecoveryRecords.bindings[0]
					Expect(bindingRecord).ToNot(BeNil())
					Expect(bindingRecord.sourceExchange).To(Equal(e.Name()))
					Expect(bindingRecord.destination).To(Equal(q.Name()))
					Expect(bindingRecord.isDestinationQueue).To(BeTrueBecause("binding was declared with a queue destination"))
					Expect(bindingRecord.bindingKey).To(Equal("test-binding-key"))

					err = conn.Management().Unbind(context.Background(), b)
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(BeEmpty())
				})

				It("should remove only the matching binding record", func() {
					q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
						Name:        generateName("test-queue-"),
						IsExclusive: true,
					})
					Expect(err).ToNot(HaveOccurred())
					e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
						Name: generateName("test-exchange-"),
					})
					Expect(err).ToNot(HaveOccurred())
					DeferCleanup(func() {
						c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
						_ = c.Management().DeleteExchange(context.Background(), e.Name())
						_ = c.Close(context.Background())
					})

					bindingPrefix := "top-recovery-test-binding"
					bindingPaths := make([]string, 0)
					for i := range 3 {
						bindingPath, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
							SourceExchange:   e.Name(),
							DestinationQueue: q.Name(),
							BindingKey:       fmt.Sprintf("%s-%d", bindingPrefix, i),
						})
						Expect(err).ToNot(HaveOccurred())
						bindingPaths = append(bindingPaths, bindingPath)
					}
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(3))

					err = conn.Management().Unbind(context.Background(), bindingPaths[1])
					Expect(err).ToNot(HaveOccurred())
					Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(2))
					Expect(conn.topologyRecoveryRecords.bindings[0].bindingKey).To(Equal(fmt.Sprintf("%s-%d", bindingPrefix, 0)))
					Expect(conn.topologyRecoveryRecords.bindings[0].isDestinationQueue).To(BeTrueBecause("binding was declared with a queue destination"))
					Expect(conn.topologyRecoveryRecords.bindings[1].bindingKey).To(Equal(fmt.Sprintf("%s-%d", bindingPrefix, 2)))
					Expect(conn.topologyRecoveryRecords.bindings[1].isDestinationQueue).To(BeTrueBecause("binding was declared with a queue destination"))
				})
			})
		})

		Context("with TopologyRecoveryOnlyTransientQueues", func() {
			var (
				conn *AmqpConnection
			)

			BeforeEach(func() {
				var err error
				conn, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryOnlyTransient})
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				Expect(conn.Close(context.Background())).To(Succeed())
			})

			It("should not recover persistent entities", func() {
				e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
					Name:         generateName("persistent-exchange"),
					IsAutoDelete: false,
				})
				Expect(err).ToNot(HaveOccurred())
				q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
					Name:         generateName("persistent-queue"),
					IsAutoDelete: false,
					IsExclusive:  false,
				})
				Expect(err).ToNot(HaveOccurred())
				b, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
					SourceExchange:   e.Name(),
					DestinationQueue: q.Name(),
					BindingKey:       "test-binding-key",
				})
				Expect(err).ToNot(HaveOccurred())
				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryOnlyTransient})
					_ = c.Management().DeleteExchange(context.Background(), e.Name())
					_ = c.Management().DeleteQueue(context.Background(), q.Name())
					_ = c.Management().Unbind(context.Background(), b)
					_ = c.Close(context.Background())
				})

				By("not keeping recovery records")
				Expect(conn.topologyRecoveryRecords.exchanges).To(BeEmpty())
				Expect(conn.topologyRecoveryRecords.queues).To(BeEmpty())
				Expect(conn.topologyRecoveryRecords.bindings).To(BeEmpty())

				By("recording only the transient entities")
				qt, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
					Name:         generateName("transient-queue"),
					IsAutoDelete: true,
					IsExclusive:  true,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(1))
				queueRecord := conn.topologyRecoveryRecords.queues[0]
				Expect(queueRecord).ToNot(BeNil())
				Expect(queueRecord.queueName).To(Equal(qt.Name()))
				Expect(queueRecord.autoDelete).ToNot(BeNil())
				Expect(*queueRecord.autoDelete).To(BeTrueBecause("queue was declared as auto delete"))
				Expect(queueRecord.exclusive).ToNot(BeNil())
				Expect(*queueRecord.exclusive).To(BeTrueBecause("queue was declared as exclusive"))

				bt, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
					SourceExchange:   e.Name(),
					DestinationQueue: qt.Name(),
					BindingKey:       "test-binding-key",
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))
				bindingRecord := conn.topologyRecoveryRecords.bindings[0]
				Expect(bindingRecord).ToNot(BeNil())
				Expect(bindingRecord.sourceExchange).To(Equal(e.Name()))
				Expect(bindingRecord.destination).To(Equal(qt.Name()))
				Expect(bindingRecord.isDestinationQueue).To(BeTrueBecause("binding was declared with a queue destination"))
				Expect(bindingRecord.bindingKey).To(Equal("test-binding-key"))
				Expect(bindingRecord.path).To(Equal(bt))
			})

			It("should record auto-delete exchanges as transient", func() {
				e, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
					Name:         generateName("transient-exchange"),
					IsAutoDelete: true,
				})
				Expect(err).ToNot(HaveOccurred())
				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryOnlyTransient})
					_ = c.Management().DeleteExchange(context.Background(), e.Name())
					_ = c.Close(context.Background())
				})

				By("keeping the auto-delete exchange record")
				Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(1))
				exchangeRecord := conn.topologyRecoveryRecords.exchanges[0]
				Expect(exchangeRecord).ToNot(BeNil())
				Expect(exchangeRecord.exchangeName).To(Equal(e.Name()))
				Expect(exchangeRecord.exchangeType).To(Equal(Direct))
				Expect(exchangeRecord.autoDelete).To(BeTrueBecause("exchange was declared as auto delete"))
			})

			It("should not record persistent exchanges", func() {
				e, err := conn.Management().DeclareExchange(context.Background(), &TopicExchangeSpecification{
					Name:         generateName("persistent-topic-exchange"),
					IsAutoDelete: false,
				})
				Expect(err).ToNot(HaveOccurred())
				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryOnlyTransient})
					_ = c.Management().DeleteExchange(context.Background(), e.Name())
					_ = c.Close(context.Background())
				})

				By("not keeping the persistent exchange record")
				Expect(conn.topologyRecoveryRecords.exchanges).To(BeEmpty())
			})
		})

		Context("with TopologyRecoveryDisabled", func() {
			var (
				conn *AmqpConnection
			)

			BeforeEach(func() {
				var err error
				conn, err = Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryDisabled})
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				Expect(conn.Close(context.Background())).To(Succeed())
			})

			It("should not record any exchanges", func() {
				e1, err := conn.Management().DeclareExchange(context.Background(), &DirectExchangeSpecification{
					Name:         generateName("persistent-exchange-disabled"),
					IsAutoDelete: false,
				})
				Expect(err).ToNot(HaveOccurred())

				e2, err := conn.Management().DeclareExchange(context.Background(), &FanOutExchangeSpecification{
					Name:         generateName("transient-exchange-disabled"),
					IsAutoDelete: true,
				})
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryDisabled})
					_ = c.Management().DeleteExchange(context.Background(), e1.Name())
					_ = c.Management().DeleteExchange(context.Background(), e2.Name())
					_ = c.Close(context.Background())
				})

				By("not keeping any exchange records regardless of auto-delete flag")
				Expect(conn.topologyRecoveryRecords.exchanges).To(BeEmpty())
			})

			It("should not record any queues", func() {
				q1, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
					Name:         generateName("persistent-queue-disabled"),
					IsAutoDelete: false,
					IsExclusive:  false,
				})
				Expect(err).ToNot(HaveOccurred())

				q2, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
					Name:         generateName("transient-queue-disabled"),
					IsAutoDelete: true,
					IsExclusive:  true,
				})
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryDisabled})
					_ = c.Management().DeleteQueue(context.Background(), q1.Name())
					_ = c.Management().DeleteQueue(context.Background(), q2.Name())
					_ = c.Close(context.Background())
				})

				By("not keeping any queue records regardless of transient flags")
				Expect(conn.topologyRecoveryRecords.queues).To(BeEmpty())
			})

			It("should not record any bindings", func() {
				q, err := conn.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{
					Name:        generateName("queue-disabled"),
					IsExclusive: true,
				})
				Expect(err).ToNot(HaveOccurred())

				b, err := conn.Management().Bind(context.Background(), &ExchangeToQueueBindingSpecification{
					SourceExchange:   "amq.direct",
					DestinationQueue: q.Name(),
					BindingKey:       "test-binding-key",
				})
				Expect(err).ToNot(HaveOccurred())

				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryDisabled})
					_ = c.Management().Unbind(context.Background(), b)
					_ = c.Management().DeleteQueue(context.Background(), q.Name())
					_ = c.Close(context.Background())
				})

				By("not keeping any binding records")
				Expect(conn.topologyRecoveryRecords.bindings).To(BeEmpty())
			})
		})

		Context("exchange recovery behavior with different types", func() {
			It("should handle all exchange types with TopologyRecoveryAllEnabled", func() {
				conn, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryAllEnabled})
				Expect(err).ToNot(HaveOccurred())
				defer conn.Close(context.Background())

				exchangeSpecs := []IExchangeSpecification{
					&DirectExchangeSpecification{
						Name:         generateName("direct-ex"),
						IsAutoDelete: false,
					},
					&TopicExchangeSpecification{
						Name:         generateName("topic-ex"),
						IsAutoDelete: false,
					},
					&FanOutExchangeSpecification{
						Name:         generateName("fanout-ex"),
						IsAutoDelete: true,
					},
					&HeadersExchangeSpecification{
						Name:         generateName("headers-ex"),
						IsAutoDelete: true,
					},
				}

				for _, spec := range exchangeSpecs {
					_, err := conn.Management().DeclareExchange(context.Background(), spec)
					Expect(err).ToNot(HaveOccurred())
				}

				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", nil)
					for _, spec := range exchangeSpecs {
						_ = c.Management().DeleteExchange(context.Background(), spec.name())
					}
					_ = c.Close(context.Background())
				})

				By("recording all exchange types")
				Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(4))

				By("verifying each exchange record")
				for i, spec := range exchangeSpecs {
					record := conn.topologyRecoveryRecords.exchanges[i]
					Expect(record.exchangeName).To(Equal(spec.name()))
					Expect(record.exchangeType).To(Equal(spec.exchangeType()))
					Expect(record.autoDelete).To(Equal(spec.isAutoDelete()))
				}
			})

			It("should handle only auto-delete exchanges with TopologyRecoveryOnlyTransientQueues", func() {
				conn, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{TopologyRecoveryOptions: TopologyRecoveryOnlyTransient})
				Expect(err).ToNot(HaveOccurred())
				defer conn.Close(context.Background())

				// Persistent exchanges - should NOT be recorded
				persistentExchanges := []IExchangeSpecification{
					&DirectExchangeSpecification{
						Name:         generateName("persistent-direct"),
						IsAutoDelete: false,
					},
					&TopicExchangeSpecification{
						Name:         generateName("persistent-topic"),
						IsAutoDelete: false,
					},
				}

				// Transient exchanges - SHOULD be recorded
				transientExchanges := []IExchangeSpecification{
					&FanOutExchangeSpecification{
						Name:         generateName("transient-fanout"),
						IsAutoDelete: true,
					},
					&HeadersExchangeSpecification{
						Name:         generateName("transient-headers"),
						IsAutoDelete: true,
					},
				}

				allExchanges := append(persistentExchanges, transientExchanges...)

				for _, spec := range allExchanges {
					_, err := conn.Management().DeclareExchange(context.Background(), spec)
					Expect(err).ToNot(HaveOccurred())
				}

				DeferCleanup(func() {
					c, _ := Dial(context.Background(), "amqp://", nil)
					for _, spec := range allExchanges {
						_ = c.Management().DeleteExchange(context.Background(), spec.name())
					}
					_ = c.Close(context.Background())
				})

				By("recording only auto-delete exchanges")
				Expect(conn.topologyRecoveryRecords.exchanges).To(HaveLen(2))

				By("verifying that only transient exchanges were recorded")
				for i, spec := range transientExchanges {
					record := conn.topologyRecoveryRecords.exchanges[i]
					Expect(record.exchangeName).To(Equal(spec.name()))
					Expect(record.exchangeType).To(Equal(spec.exchangeType()))
					Expect(record.autoDelete).To(BeTrue())
				}
			})
		})

		Context("end-to-end tests", func() {
			var (
				env         *Environment
				containerId string
			)

			BeforeEach(func() {
				containerId = CurrentSpecReport().LeafNodeText
				env = NewEnvironment("amqp://", &AmqpConnOptions{
					TopologyRecoveryOptions: TopologyRecoveryOnlyTransient,
					ContainerID:             containerId,
					SASLType:                amqp.SASLTypeAnonymous(),
					RecoveryConfiguration: &RecoveryConfiguration{
						ActiveRecovery:           true,
						BackOffReconnectInterval: 2 * time.Second,
						MaxReconnectAttempts:     5,
					},
					Id: containerId,
				})
			})

			AfterEach(func(ctx context.Context) {
				env.CloseConnections(ctx)
			})

			It("should recover the topology", func(ctx context.Context) {
				conn, err := env.NewConnection(ctx)
				Expect(err).ToNot(HaveOccurred())

				ch := make(chan *StateChanged, 1)
				conn.NotifyStatusChange(ch)

				exchange, err := conn.Management().DeclareExchange(ctx, &DirectExchangeSpecification{
					Name:         generateName("direct-ad-exchange"),
					IsAutoDelete: true,
				})
				Expect(err).ToNot(HaveOccurred())

				queue, err := conn.Management().DeclareQueue(ctx, &ClassicQueueSpecification{
					Name:         generateName("classic-ad-excl-queue"),
					IsAutoDelete: true,
					IsExclusive:  true,
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = conn.Management().Bind(ctx, &ExchangeToQueueBindingSpecification{
					SourceExchange:   exchange.Name(),
					DestinationQueue: queue.Name(),
					BindingKey:       "test-binding-key",
				})
				Expect(err).ToNot(HaveOccurred())

				producer, err := conn.NewPublisher(
					ctx,
					&ExchangeAddress{Exchange: exchange.Name(), Key: "test-binding-key"},
					nil,
				)
				Expect(err).ToNot(HaveOccurred())

				msg := NewMessage([]byte("hello"))
				result, err := producer.Publish(ctx, msg)
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Outcome).To(Equal(&StateAccepted{}))

				consumer, err := conn.NewConsumer(ctx, queue.Name(), nil)
				Expect(err).ToNot(HaveOccurred())

				msgReceived, err := consumer.Receive(ctx)
				Expect(err).ToNot(HaveOccurred())
				Expect(msgReceived.Message().GetData()).To(Equal([]byte("hello")))
				Expect(msgReceived.Accept(ctx)).To(Succeed())

				// Drop connection
				Eventually(func() error {
					return testhelper.DropConnectionContainerID(containerId)
				}).WithTimeout(5*time.Second).WithPolling(400*time.Millisecond).
					Should(Succeed(), "expected connection to be closed")
				stateChange := new(StateChanged)
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive(&stateChange))
				Expect(stateChange.From).To(Equal(&StateOpen{}))
				Expect(stateChange.To).To(BeAssignableToTypeOf(&StateClosed{}))

				// Receive reconnecting state
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive())

				By("recovering the connection")
				// Await reconnection
				Eventually(func() (bool, error) {
					conn, err := testhelper.GetConnectionByContainerID(containerId)
					return conn != nil, err
				}).WithTimeout(6 * time.Second).WithPolling(400 * time.Millisecond).
					Should(BeTrueBecause("expected connection to be reconnected"))
				stateChange = new(StateChanged)
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive(&stateChange))
				Expect(stateChange.To).To(Equal(&StateOpen{}))

				By("publishing and consuming again")
				// Publish again
				msg = NewMessage([]byte("hello again"))
				result, err = producer.Publish(ctx, msg)
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Outcome).To(Equal(&StateAccepted{}))

				// Consume again
				msgReceived, err = consumer.Receive(ctx)
				Expect(err).ToNot(HaveOccurred())
				Expect(msgReceived.Message().GetData()).To(Equal([]byte("hello again")))
				Expect(msgReceived.Accept(ctx)).To(Succeed())

				Expect(conn.Close(ctx)).To(Succeed())
			})

			It("should not duplicate recovery records", func(ctx context.Context) {
				conn, err := env.NewConnection(ctx)
				Expect(err).ToNot(HaveOccurred())

				q, err := conn.Management().DeclareQueue(ctx, &ClassicQueueSpecification{
					Name:         generateName("non-duplicate-records-queue"),
					IsAutoDelete: true,
					IsExclusive:  true,
				})
				Expect(err).ToNot(HaveOccurred())
				_, err = conn.Management().Bind(ctx, &ExchangeToQueueBindingSpecification{
					SourceExchange:   "amq.direct",
					DestinationQueue: q.Name(),
					BindingKey:       "non-duplicate-records-binding-key",
				})
				Expect(err).ToNot(HaveOccurred())

				Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(1))
				Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))

				producer, err := conn.NewPublisher(ctx, &ExchangeAddress{Exchange: "amq.direct", Key: "non-duplicate-records-binding-key"}, nil)
				Expect(err).ToNot(HaveOccurred())

				msg := NewMessage([]byte("hello"))
				result, err := producer.Publish(ctx, msg)
				Expect(err).ToNot(HaveOccurred())
				Expect(result.Outcome).To(Equal(&StateAccepted{}))

				ch := make(chan *StateChanged, 1)
				conn.NotifyStatusChange(ch)

				// Drop connection
				Eventually(func() error {
					return testhelper.DropConnectionContainerID(containerId)
				}).WithTimeout(5*time.Second).WithPolling(400*time.Millisecond).
					Should(Succeed(), "expected connection to be closed")
				stateChange := new(StateChanged)
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive(&stateChange))
				Expect(stateChange.From).To(Equal(&StateOpen{}))
				Expect(stateChange.To).To(BeAssignableToTypeOf(&StateClosed{}))

				// Receive reconnecting state
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive())

				By("recovering the connection")
				// Await reconnection
				Eventually(func() (bool, error) {
					conn, err := testhelper.GetConnectionByContainerID(containerId)
					return conn != nil, err
				}).WithTimeout(6 * time.Second).WithPolling(400 * time.Millisecond).
					Should(BeTrueBecause("expected connection to be reconnected"))
				stateChange = new(StateChanged)
				Eventually(ch).Within(5 * time.Second).WithPolling(400 * time.Millisecond).
					Should(Receive(&stateChange))
				Expect(stateChange.To).To(Equal(&StateOpen{}))

				By("verifying that the recovery records are not duplicated")
				Expect(conn.topologyRecoveryRecords.queues).To(HaveLen(1))
				Expect(conn.topologyRecoveryRecords.bindings).To(HaveLen(1))

				_ = conn.Close(ctx)
			})
		})
	})
})
