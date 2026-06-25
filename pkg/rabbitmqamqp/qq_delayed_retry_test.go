package rabbitmqamqp

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The tests in this file exercise the four QuorumQueueDelayedRetryType values
// introduced in RabbitMQ 4.3:
//
//   - QuorumQueueDelayedRetryDisabled – no delay is applied regardless of disposition
//   - QuorumQueueDelayedRetryAll      – every return (released or modified) is delayed
//   - QuorumQueueDelayedRetryFailed   – only modified{delivery-failed=true} triggers the delay
//   - QuorumQueueDelayedRetryReturned – only non-failed returns (released / modified{delivery-failed=false}) trigger the delay
//
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries

const (
	delayedRetryMin       = 500 * time.Millisecond
	delayedRetryTolerance = 480 * time.Millisecond // allow 20 ms of clock skew

	// "immediate" upper bound: must be well below delayedRetryMin so that a false
	// delay would be caught, yet generous enough to survive a slow CI runner.
	delayedRetryImmediate = 400 * time.Millisecond
)

var _ = Describe("Quorum Queue Delayed Retry types", func() {

	var connection *AmqpConnection

	BeforeEach(func() {
		var err error
		connection, err = Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())

		ver, _ := connection.Properties()["version"].(string)
		if !isVersionGreaterOrEqual(extractVersion(ver), "4.3.0") {
			Skip("requires RabbitMQ 4.3 or later for quorum queue delayed retry")
		}
	})

	AfterEach(func() {
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	// -------------------------------------------------------------------------
	// QuorumQueueDelayedRetryDisabled
	// -------------------------------------------------------------------------

	Describe("QuorumQueueDelayedRetryDisabled", func() {
		It("should NOT delay redelivery when delayed retry type is disabled",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-disabled")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryDisabled,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, false)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				// With "disabled", the broker must not hold the message aside.
				Expect(time.Since(startTime)).To(BeNumerically("<", delayedRetryImmediate))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(10*time.Second))
	})

	// -------------------------------------------------------------------------
	// QuorumQueueDelayedRetryAll
	// -------------------------------------------------------------------------

	Describe("QuorumQueueDelayedRetryAll", func() {
		It("should delay redelivery when deliveryFailed=false (modified, non-failure return)",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-all-not-failed")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryAll,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, false)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				// "all" must delay every return, including non-failure ones.
				Expect(time.Since(startTime)).To(BeNumerically(">=", delayedRetryTolerance))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(15*time.Second))

		It("should delay redelivery when deliveryFailed=true (modified, failure return)",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-all-failed")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryAll,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, true)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				// "all" must delay every return, including failure ones.
				Expect(time.Since(startTime)).To(BeNumerically(">=", delayedRetryTolerance))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(15*time.Second))
	})

	// -------------------------------------------------------------------------
	// QuorumQueueDelayedRetryFailed
	// -------------------------------------------------------------------------

	Describe("QuorumQueueDelayedRetryFailed", func() {
		It("should delay redelivery when deliveryFailed=true",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-failed-true")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryFailed,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				// delivery-failed=true increments delivery-count → delay must be applied.
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, true)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				Expect(time.Since(startTime)).To(BeNumerically(">=", delayedRetryTolerance))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(15*time.Second))

		It("should NOT delay redelivery when deliveryFailed=false",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-failed-not-false")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryFailed,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				// delivery-failed=false does NOT increment delivery-count → no delay for "failed" type.
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, false)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				Expect(time.Since(startTime)).To(BeNumerically("<", delayedRetryImmediate))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(10*time.Second))
	})

	// -------------------------------------------------------------------------
	// QuorumQueueDelayedRetryReturned
	// -------------------------------------------------------------------------

	Describe("QuorumQueueDelayedRetryReturned", func() {
		It("should delay redelivery when deliveryFailed=false (modified, non-failure return)",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-returned-false")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryReturned,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				// delivery-failed=false increments acquired-count only → delay for "returned" type.
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, false)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				Expect(time.Since(startTime)).To(BeNumerically(">=", delayedRetryTolerance))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(15*time.Second))

		It("should delay redelivery when using Requeue() (AMQP released outcome)",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-returned-requeue")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryReturned,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				// Requeue() sends AMQP released → increments acquired-count only → delay for "returned".
				Expect(dc.Requeue(ctx)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				Expect(time.Since(startTime)).To(BeNumerically(">=", delayedRetryTolerance))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(15*time.Second))

		It("should NOT delay redelivery when deliveryFailed=true",
			func(ctx SpecContext) {
				qName := generateNameWithDateTime("qq-delayed-retry-returned-true")
				DeferCleanup(func() {
					_ = connection.Management().DeleteQueue(context.Background(), qName)
				})

				_, err := connection.Management().DeclareQueue(ctx, &QuorumQueueSpecification{
					Name:             qName,
					DelayedRetryType: QuorumQueueDelayedRetryReturned,
					DelayedRetryMin:  delayedRetryMin,
				})
				Expect(err).To(BeNil())

				publishMessages(qName, 1)

				consumer, err := connection.NewConsumer(ctx, qName, nil)
				Expect(err).To(BeNil())
				DeferCleanup(func() { _ = consumer.Close(context.Background()) })

				dc, err := consumer.Receive(ctx)
				Expect(err).To(BeNil())

				startTime := time.Now()
				// delivery-failed=true increments delivery-count → this is a "failed" return,
				// not a "returned" return, so the delay must NOT be applied.
				Expect(dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, nil, true)).To(BeNil())

				receiveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				DeferCleanup(cancel)

				redelivered, err := consumer.Receive(receiveCtx)
				Expect(err).To(BeNil())
				Expect(time.Since(startTime)).To(BeNumerically("<", delayedRetryImmediate))
				Expect(redelivered.Accept(ctx)).To(BeNil())
			}, SpecTimeout(10*time.Second))
	})
})
