package rabbitmqamqp

import (
	"context"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// ─── unit tests ──────────────────────────────────────────────────────────────
// These describe blocks do NOT connect to a broker.

var _ = Describe("QuorumQueueSpecification consumer timeout", func() {
	It("sets x-consumer-timeout in milliseconds when ConsumerTimeout is set", func() {
		spec := &QuorumQueueSpecification{
			Name:            "my-qq",
			ConsumerTimeout: 30 * time.Second,
		}
		args := spec.buildArguments()
		Expect(args["x-consumer-timeout"]).To(Equal(int64(30_000)))
		Expect(args["x-queue-type"]).To(Equal("quorum"))
	})

	It("does not set x-consumer-timeout when ConsumerTimeout is zero", func() {
		spec := &QuorumQueueSpecification{Name: "my-qq"}
		args := spec.buildArguments()
		Expect(args).ToNot(HaveKey("x-consumer-timeout"))
	})

	It("combines ConsumerTimeout with other quorum arguments", func() {
		spec := &QuorumQueueSpecification{
			Name:              "my-qq",
			DeliveryLimit:     5,
			TargetClusterSize: 3,
			ConsumerTimeout:   90 * time.Second,
		}
		args := spec.buildArguments()
		Expect(args["x-consumer-timeout"]).To(Equal(int64(90_000)))
		Expect(args["x-max-delivery-limit"]).To(BeNil()) // field is named DeliveryLimit → x-delivery-limit
		Expect(args["x-delivery-limit"]).To(Equal(int64(5)))
		Expect(args["x-quorum-target-group-size"]).To(Equal(int64(3)))
		Expect(args["x-queue-type"]).To(Equal("quorum"))
	})

	It("fails validate when ConsumerTimeout is set on RabbitMQ < 4.3", func() {
		spec := &QuorumQueueSpecification{
			Name:            "my-qq",
			ConsumerTimeout: 5 * time.Second,
		}
		err := spec.validate(&featuresAvailable{is43rMore: false})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("4.3"))
	})

	It("passes validate when ConsumerTimeout is set on RabbitMQ >= 4.3", func() {
		spec := &QuorumQueueSpecification{
			Name:            "my-qq",
			ConsumerTimeout: 5 * time.Second,
		}
		Expect(spec.validate(&featuresAvailable{is43rMore: true})).To(BeNil())
	})

	It("passes validate when ConsumerTimeout is zero regardless of version", func() {
		spec := &QuorumQueueSpecification{Name: "my-qq"}
		Expect(spec.validate(&featuresAvailable{is43rMore: false})).To(BeNil())
		Expect(spec.validate(&featuresAvailable{is43rMore: true})).To(BeNil())
		Expect(spec.validate(nil)).To(BeNil())
	})
})

var _ = Describe("JMSQueueSpecification consumer timeout", func() {
	It("sets x-consumer-timeout in milliseconds when ConsumerTimeout is set", func() {
		spec := &JMSQueueSpecification{
			Name:            "my-jms",
			ConsumerTimeout: 2 * time.Minute,
		}
		args := spec.buildArguments()
		Expect(args["x-consumer-timeout"]).To(Equal(int64(120_000)))
		Expect(args["x-queue-type"]).To(Equal("jms"))
	})

	It("does not set x-consumer-timeout when ConsumerTimeout is zero", func() {
		spec := &JMSQueueSpecification{Name: "my-jms"}
		args := spec.buildArguments()
		Expect(args).ToNot(HaveKey("x-consumer-timeout"))
	})
})

var _ = Describe("ConsumerOptions consumer timeout validation", func() {
	It("rejects ConsumerTimeout with DirectReplyTo settle strategy", func() {
		opts := &ConsumerOptions{
			SettleStrategy:  DirectReplyTo,
			ConsumerTimeout: 10 * time.Second,
		}
		// Provide a 4.3+ featureSet so only the consumer-timeout rule fires.
		err := opts.validate(&featuresAvailable{is42rMore: true, is43rMore: true})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("DirectReplyTo"))
	})

	It("rejects OnDeliveryRelease on RabbitMQ < 4.3", func() {
		opts := &ConsumerOptions{
			OnDeliveryRelease: func(_ IDeliveryContext, _ *amqp.Message) {},
		}
		err := opts.validate(&featuresAvailable{is43rMore: false})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("4.3"))
	})

	It("accepts OnDeliveryRelease on RabbitMQ >= 4.3", func() {
		opts := &ConsumerOptions{
			OnDeliveryRelease: func(_ IDeliveryContext, _ *amqp.Message) {},
		}
		Expect(opts.validate(&featuresAvailable{is43rMore: true})).To(BeNil())
	})

	It("accepts ConsumerTimeout with ExplicitSettle on RabbitMQ >= 4.3", func() {
		opts := &ConsumerOptions{
			SettleStrategy:  ExplicitSettle,
			ConsumerTimeout: 30 * time.Second,
		}
		Expect(opts.validate(&featuresAvailable{is43rMore: true})).To(BeNil())
	})
})

var _ = Describe("TimedOutDeliveryContext", func() {
	// Build a minimal TimedOutDeliveryContext without a real receiver/message;
	// the restricted methods check the error before touching those fields.
	var ctx *TimedOutDeliveryContext

	BeforeEach(func() {
		ctx = &TimedOutDeliveryContext{
			receiver:         nil,
			message:          nil,
			metricsCollector: DefaultMetricsCollector(),
		}
	})

	It("Discard returns ErrDeliveryReleaseInvalidOperation", func() {
		err := ctx.Discard(context.Background(), nil)
		Expect(err).To(MatchError(ErrDeliveryReleaseInvalidOperation))
	})

	It("DiscardWithAnnotations returns ErrDeliveryReleaseInvalidOperation", func() {
		err := ctx.DiscardWithAnnotations(context.Background(), amqp.Annotations{"x-reason": "test"})
		Expect(err).To(MatchError(ErrDeliveryReleaseInvalidOperation))
	})

	It("Requeue returns ErrDeliveryReleaseInvalidOperation", func() {
		err := ctx.Requeue(context.Background())
		Expect(err).To(MatchError(ErrDeliveryReleaseInvalidOperation))
	})

	It("RequeueWithAnnotations returns ErrDeliveryReleaseInvalidOperation", func() {
		err := ctx.RequeueWithAnnotations(context.Background(), amqp.Annotations{"x-reason": "retry"})
		Expect(err).To(MatchError(ErrDeliveryReleaseInvalidOperation))
	})

	It("Message returns the stored message", func() {
		msg := &amqp.Message{}
		ctx.message = msg
		Expect(ctx.Message()).To(BeIdenticalTo(msg))
	})
})

var _ = Describe("setConsumerTimeoutProperty", func() {
	It("sets rabbitmq:consumer-timeout in the receiver properties", func() {
		opts := &amqp.ReceiverOptions{}
		consumerOpts := &ConsumerOptions{ConsumerTimeout: 45 * time.Second}
		setConsumerTimeoutProperty(opts, consumerOpts)
		Expect(opts.Properties).To(HaveKey(rabbitmqConsumerTimeoutProperty))
		Expect(opts.Properties[rabbitmqConsumerTimeoutProperty]).To(Equal(uint64(45_000)))
	})

	It("does not set the property when ConsumerTimeout is zero", func() {
		opts := &amqp.ReceiverOptions{}
		setConsumerTimeoutProperty(opts, &ConsumerOptions{})
		Expect(opts.Properties).To(BeNil())
	})

	It("does not set the property for non-ConsumerOptions types", func() {
		opts := &amqp.ReceiverOptions{}
		setConsumerTimeoutProperty(opts, &StreamConsumerOptions{})
		Expect(opts.Properties).To(BeNil())
	})

	It("preserves existing properties when adding consumer timeout", func() {
		opts := &amqp.ReceiverOptions{
			Properties: map[string]any{"paired": true},
		}
		consumerOpts := &ConsumerOptions{ConsumerTimeout: 10 * time.Second}
		setConsumerTimeoutProperty(opts, consumerOpts)
		Expect(opts.Properties).To(HaveKey("paired"))
		Expect(opts.Properties[rabbitmqConsumerTimeoutProperty]).To(Equal(uint64(10_000)))
	})
})

var _ = Describe("setDeliveryReleaseHandler", func() {
	It("does not set OnDeliveryStateChanged when OnDeliveryRelease is nil", func() {
		opts := &amqp.ReceiverOptions{}
		setDeliveryReleaseHandler(opts, &ConsumerOptions{}, &Consumer{})
		Expect(opts.OnDeliveryStateChanged).To(BeNil())
	})

	It("sets OnDeliveryStateChanged when OnDeliveryRelease is configured", func() {
		opts := &amqp.ReceiverOptions{}
		called := false
		consumerOpts := &ConsumerOptions{
			OnDeliveryRelease: func(_ IDeliveryContext, _ *amqp.Message) {
				called = true
			},
		}
		setDeliveryReleaseHandler(opts, consumerOpts, &Consumer{
			connection: &AmqpConnection{metricsCollector: DefaultMetricsCollector()},
		})
		Expect(opts.OnDeliveryStateChanged).ToNot(BeNil())
		_ = called // used in integration test
	})

	It("does not set OnDeliveryStateChanged for StreamConsumerOptions", func() {
		opts := &amqp.ReceiverOptions{}
		setDeliveryReleaseHandler(opts, &StreamConsumerOptions{}, &Consumer{})
		Expect(opts.OnDeliveryStateChanged).To(BeNil())
	})
})

// ─── integration tests ───────────────────────────────────────────────────────
// These require a running RabbitMQ 4.3+ broker.

var _ = Describe("Consumer timeout integration", func() {
	It("creates a consumer with per-consumer ConsumerTimeout attach property", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = connection.Close(context.Background()) })

		qName := generateNameWithDateTime("consumer_timeout_attach")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			_ = connection.Management().DeleteQueue(context.Background(), qName)
		})

		consumer, err := connection.NewConsumer(context.Background(), qName, &ConsumerOptions{
			ConsumerTimeout: 30 * time.Second,
		})
		Expect(err).To(BeNil())
		Expect(consumer).ToNot(BeNil())
		Expect(consumer.Close(context.Background())).To(BeNil())
	})

	It("rejects ConsumerTimeout combined with DirectReplyTo", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = connection.Close(context.Background()) })

		qName := generateNameWithDateTime("consumer_timeout_direct_reply")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			_ = connection.Management().DeleteQueue(context.Background(), qName)
		})

		_, err = connection.NewConsumer(context.Background(), qName, &ConsumerOptions{
			SettleStrategy:  DirectReplyTo,
			ConsumerTimeout: 10 * time.Second,
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("DirectReplyTo"))
	})

	It("rejects OnDeliveryRelease on RabbitMQ < 4.3 (skipped when >= 4.3)", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = connection.Close(context.Background()) })

		ver, _ := connection.Properties()["version"].(string)
		if isVersionGreaterOrEqual(extractVersion(ver), "4.3.0") {
			Skip("broker is 4.3+, OnDeliveryRelease is supported — skip the rejection test")
		}

		qName := generateNameWithDateTime("consumer_timeout_too_old")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			_ = connection.Management().DeleteQueue(context.Background(), qName)
		})

		_, err = connection.NewConsumer(context.Background(), qName, &ConsumerOptions{
			OnDeliveryRelease: func(_ IDeliveryContext, _ *amqp.Message) {},
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("4.3"))
	})

	It("declares a quorum queue with x-consumer-timeout and reads it back", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = connection.Close(context.Background()) })

		ver, _ := connection.Properties()["version"].(string)
		if !isVersionGreaterOrEqual(extractVersion(ver), "4.3.0") {
			Skip("requires RabbitMQ 4.3+ for x-consumer-timeout")
		}

		qName := generateNameWithDateTime("qq_consumer_timeout_arg")
		info, err := connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name:            qName,
			ConsumerTimeout: 15 * time.Minute,
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			_ = connection.Management().DeleteQueue(context.Background(), qName)
		})

		Expect(info.Arguments()["x-consumer-timeout"]).To(Equal(int64(900_000)))
	})

	It("fires OnDeliveryRelease when broker releases due to queue-level consumer timeout", func(ctx SpecContext) {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = connection.Close(context.Background()) })

		ver, _ := connection.Properties()["version"].(string)
		if !isVersionGreaterOrEqual(extractVersion(ver), "4.3.0") {
			Skip("requires RabbitMQ 4.3+ for consumer timeout feature")
		}

		// Use a queue-level ConsumerTimeout – this is the established way to trigger
		// broker-initiated releases. The per-consumer attach property may additionally
		// lower the effective timeout, but the queue-level setting drives the release.
		queueTimeout := 5 * time.Second
		qName := generateNameWithDateTime("qq_consumer_timeout_release")
		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name:            qName,
			ConsumerTimeout: queueTimeout,
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() {
			_ = connection.Management().DeleteQueue(context.Background(), qName)
		})

		releaseCh := make(chan struct{}, 1)
		releaseCallbackFired := false

		consumer, err := connection.NewConsumer(context.Background(), qName, &ConsumerOptions{
			OnDeliveryRelease: func(deliveryCtx IDeliveryContext, _ *amqp.Message) {
				releaseCallbackFired = true
				// Accept() unlocks the consumer after the broker-initiated release.
				_ = deliveryCtx.Accept(context.Background())
				releaseCh <- struct{}{}
			},
		})
		Expect(err).To(BeNil())
		DeferCleanup(func() { _ = consumer.Close(context.Background()) })

		// Publish one message.
		publishMessages(qName, 1, "timeout-test-msg")

		// Receive the message but never settle it so the queue-level timeout fires.
		heldDeliveryCh := make(chan IDeliveryContext, 1)
		go func() {
			dc, recvErr := consumer.Receive(ctx)
			if recvErr != nil {
				return
			}
			heldDeliveryCh <- dc // hand it off so the goroutine stays alive, holding the delivery
		}()

		// Wait for the delivery to be held.
		Eventually(heldDeliveryCh, 10*time.Second, 100*time.Millisecond).Should(Receive())

		// The broker releases after queueTimeout – wait up to 3× that for the callback.
		Eventually(releaseCh, queueTimeout*3, 100*time.Millisecond).Should(Receive())
		Expect(releaseCallbackFired).To(BeTrue())
	}, SpecTimeout(90*time.Second))
})
