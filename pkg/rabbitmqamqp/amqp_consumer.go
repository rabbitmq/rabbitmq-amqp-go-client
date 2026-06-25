package rabbitmqamqp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// IDeliveryContext represents a delivery context for received messages.
// It provides methods to access the message and settle it (accept, discard, requeue).
type IDeliveryContext interface {
	Message() *amqp.Message
	Accept(ctx context.Context) error
	Discard(ctx context.Context, e *amqp.Error) error
	DiscardWithAnnotations(ctx context.Context, annotations amqp.Annotations) error
	Requeue(ctx context.Context) error
	//RequeueWithAnnotations requeue the message with annotations. DeliveryFailed is implicit set to
	// false.
	// The API is provided for convenience and for compatibility, but it is recommended to use RequeueWithAnnotationsAndDeliveryFailed instead.
	// It will be deprecated and removed in the future.
	RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error
	// RequeueWithAnnotationsAndDeliveryFailed requeue the message with annotations and deliveryFailed flag. DeliveryFailed is implicit set to
	// true if deliveryFailed is true, false otherwise.
	RequeueWithAnnotationsAndDeliveryFailed(ctx context.Context, annotations amqp.Annotations, deliveryFailed bool) error // default true
	// DelayRetry is a helper function to requeue the message with a delay.
	// for delayed retries feature. https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries
	// It is RequeueWithAnnotationsAndDeliveryFailed with the x-opt-delivery-time annotation set to the current time + delay.
	// DelayRetry is per message, and it is not needed to configure any DelayRetry policy in the queue
	DelayRetry(ctx context.Context, delay time.Duration, deliveryFailed bool) error
}

// DeliveryContext holds the receiver and message for a single AMQP delivery.
// It implements IDeliveryContext and is used when the consumer operates in
// at-least-once (explicit settle) mode.
type DeliveryContext struct {
	receiver         *amqp.Receiver
	message          *amqp.Message
	metricsCollector MetricsCollector
	consumeCtx       ConsumeContext // For OTEL semantic convention attributes
}

// Message returns the received AMQP message.
func (dc *DeliveryContext) Message() *amqp.Message {
	return dc.message
}

// Accept the message (AMQP 1.0 <code>accepted</code> outcome).
//
// This means the message has been processed and the broker can delete it.
func (dc *DeliveryContext) Accept(ctx context.Context) error {
	err := dc.receiver.AcceptMessage(ctx, dc.message)
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeAccepted, dc.consumeCtx)
	}
	return err
}

// Discard the message (AMQP 1.0 <code>rejected</code> outcome).
//
// This means the message cannot be processed because it is invalid, the broker can drop it
// or dead-letter it if it is configured.
func (dc *DeliveryContext) Discard(ctx context.Context, e *amqp.Error) error {
	err := dc.receiver.RejectMessage(ctx, dc.message, e)
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeDiscarded, dc.consumeCtx)
	}
	return err
}

// copyAnnotations helper function to copy annotations
func copyAnnotations(annotations amqp.Annotations) (amqp.Annotations, error) {
	if err := validateMessageAnnotations(annotations); err != nil {
		return nil, err
	}
	destination := make(amqp.Annotations)
	for key, value := range annotations {
		destination[key] = value
	}
	return destination, nil
}

// streamOffsetFromAnnotation decodes RabbitMQ stream offset annotation values without panicking
// on unexpected AMQP decoded numeric types.
func streamOffsetFromAnnotation(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case int16:
		return int64(t), true
	case int8:
		return int64(t), true
	case int:
		return int64(t), true
	case uint64:
		if t > uint64(1<<63-1) {
			return 0, false
		}
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint:
		return int64(t), true
	default:
		return 0, false
	}
}

// DiscardWithAnnotations the message with annotations to combine with the existing message annotations.
// <p>This means the message cannot be processed because it is invalid, the broker can drop it
// or dead-letter it if it is configured.
//
// <p>Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
// Annotation keys the broker understands start with <code>x-</code>, but not with <code>x-opt-
// </code>.
//
// <p>This maps to the AMQP 1.0 <code>
// modified{delivery-failed = true, undeliverable-here = true}</code> outcome.
//
// <p><b>Only quorum queues support the modification of message annotations with the <code>
// modified</code> outcome.</b>
//
// annotations message annotations to combine with existing ones
// @see <a
//
//	href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
//	1.0 <code>modified</code> outcome</a>
//
// @see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support in RabbitMQ</a>
func (dc *DeliveryContext) DiscardWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	destination, err := copyAnnotations(annotations)
	if err != nil {
		return err
	}
	err = dc.receiver.ModifyMessage(ctx, dc.message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    true,
		UndeliverableHere: true,
		Annotations:       destination,
	})
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeDiscarded, dc.consumeCtx)
	}
	return err
}

// Requeue the message (AMQP 1.0 <code>released</code> outcome).
//
// This means the message has not been processed and the broker can requeue it and deliver it
// to the same or a different consumer.
func (dc *DeliveryContext) Requeue(ctx context.Context) error {
	err := dc.receiver.ReleaseMessage(ctx, dc.message)
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeRequeued, dc.consumeCtx)
	}
	return err
}

// RequeueWithAnnotations the message with annotations to combine with the existing message annotations.
//
// This means the message has not been processed and the broker can requeue it and deliver it
// to the same or a different consumer.
//
// Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
// Annotation keys the broker understands start with <code>x-</code>, but not with <code>x-opt-
// </code>.
//
// This maps to the AMQP 1.0 <code>
// modified{delivery-failed = false, undeliverable-here = false}</code> outcome.
//
// Only quorum queues support the modification of message annotations with the <code>
// modified</code> outcome.
//
// annotations message annotations to combine with existing ones
// see <a
//
//	href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
//	1.0 <code>modified</code> outcome</a>
//
// see <a href="https://www.rabbitmq.com/docs/amqp#modified-outcome">Modified Outcome Support in RabbitMQ</a>
func (dc *DeliveryContext) RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	destination, err := copyAnnotations(annotations)
	if err != nil {
		return err
	}
	err = dc.receiver.ModifyMessage(ctx, dc.message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    false,
		UndeliverableHere: false,
		Annotations:       destination,
	})
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeRequeued, dc.consumeCtx)
	}
	return err
}

// RequeueWithAnnotationsAndDeliveryFailed requeue the message with annotations and controls the
// delivery-failed flag on the AMQP 1.0 modified outcome.
//
// When deliveryFailed is true the broker increments the message's delivery-count and, if the
// queue is configured with x-delayed-retry-type=failed, applies the configured linear back-off
// before redelivering the message.
//
// Application-specific annotation keys must start with the x-opt- prefix.
// Annotation keys the broker understands start with x-, but not with x-opt-.
//
// This maps to modified{delivery-failed = <deliveryFailed>, undeliverable-here = false}.
//
// Only quorum queues support the modification of message annotations with the modified outcome.
func (dc *DeliveryContext) RequeueWithAnnotationsAndDeliveryFailed(ctx context.Context, annotations amqp.Annotations, deliveryFailed bool) error {
	destination, err := copyAnnotations(annotations)
	if err != nil {
		return err
	}
	err = dc.receiver.ModifyMessage(ctx, dc.message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    deliveryFailed,
		UndeliverableHere: false,
		Annotations:       destination,
	})
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeRequeued, dc.consumeCtx)
	}
	return err
}

// DelayRetry requeue the message with a per-message delivery delay.
// It sets the x-opt-delivery-time annotation to the absolute Unix timestamp (milliseconds)
// of time.Now()+delay, triggering per-message delivery-time override on the broker side.
// The deliveryFailed flag maps directly to modified{delivery-failed}.
//
// This is equivalent to calling RequeueWithAnnotationsAndDeliveryFailed with
// {"x-opt-delivery-time": <now+delay in ms>}.
//
// See https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#delayed-retries
func (dc *DeliveryContext) DelayRetry(ctx context.Context, delay time.Duration, deliveryFailed bool) error {
	annotations := amqp.Annotations{
		"x-opt-delivery-time": time.Now().Add(delay).UnixMilli(),
	}
	return dc.RequeueWithAnnotationsAndDeliveryFailed(ctx, annotations, deliveryFailed)
}

// ErrPreSettledMessageDisposed is returned by PreSettledDeliveryContext settlement methods:
// the broker has already settled the delivery, so the application cannot accept/reject/release it.
var ErrPreSettledMessageDisposed = errors.New("auto-settle on, message is already disposed")

// PreSettledDeliveryContext represents a delivery context for pre-settled messages.
// All settlement methods throw errors since the message is already settled.
type PreSettledDeliveryContext struct {
	message *amqp.Message
}

// Message returns the received AMQP message.
func (dc *PreSettledDeliveryContext) Message() *amqp.Message {
	return dc.message
}

// Accept the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) Accept(_ context.Context) error {
	return ErrPreSettledMessageDisposed
}

// Discard the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) Discard(_ context.Context, _ *amqp.Error) error {
	return ErrPreSettledMessageDisposed
}

// DiscardWithAnnotations the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) DiscardWithAnnotations(_ context.Context, _ amqp.Annotations) error {
	return ErrPreSettledMessageDisposed
}

// Requeue the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) Requeue(_ context.Context) error {
	return ErrPreSettledMessageDisposed
}

// RequeueWithAnnotations the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) RequeueWithAnnotations(_ context.Context, _ amqp.Annotations) error {
	return ErrPreSettledMessageDisposed
}

// RequeueWithAnnotationsAndDeliveryFailed the message is already settled, so this method returns an error.
func (dc *PreSettledDeliveryContext) RequeueWithAnnotationsAndDeliveryFailed(_ context.Context, _ amqp.Annotations, _ bool) error {
	return ErrPreSettledMessageDisposed
}

// DelayRetry the message is already settled, so this method logs the error and is a no-op.
func (dc *PreSettledDeliveryContext) DelayRetry(_ context.Context, _ time.Duration, _ bool) error {
	return ErrPreSettledMessageDisposed
}

type consumerState byte

const (
	consumerStateRunning consumerState = iota
	consumerStatePausing
	consumerStatePaused
)

// Consumer represents an active AMQP 1.0 message consumer attached to a queue or stream.
// It wraps the underlying go-amqp Receiver and provides settlement helpers (Accept, Discard,
// Requeue) via the IDeliveryContext returned from Receive.
//
// Use AmqpConnection.NewConsumer to create a Consumer. Call Receive in a loop to process
// messages and Close when the consumer is no longer needed.
type Consumer struct {
	receiver       atomic.Pointer[amqp.Receiver]
	connection     *AmqpConnection
	options        IConsumerOptions
	optionsMu      sync.RWMutex // serializes options updates with Receive reading pre-settled / link-related options
	destinationAdd string
	id             string

	/*
		currentOffset is the current offset of the consumer. It is valid only for the stream consumers.
		it is used to keep track of the last message that was consumed by the consumer.
		so in case of restart the consumer can start from the last message that was consumed.
		For the AMQP queues it is just ignored.
	*/
	currentOffset atomic.Int64

	// state holds consumerState values; accessed only via atomic loads/stores.
	state atomic.Uint32

	// see GetQueue method for more details.
	queue string
}

// Id returns the unique identifier of this consumer.
// If no custom ID was provided via ConsumerOptions, a random UUID prefixed with "consumer-" is used.
func (c *Consumer) Id() string {
	return c.id
}

// rabbitmqActiveFromLinkState decodes broker FLOW link-state values for rabbitmq:active.
func rabbitmqActiveFromLinkState(v any) (active bool, ok bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case int64:
		return t != 0, true
	case int32:
		return t != 0, true
	case int16:
		return t != 0, true
	case int8:
		return t != 0, true
	case int:
		return t != 0, true
	case uint64:
		return t != 0, true
	case uint32:
		return t != 0, true
	case uint16:
		return t != 0, true
	case uint8:
		return t != 0, true
	case uint:
		return t != 0, true
	case float64:
		return t != 0, true
	case float32:
		return t != 0, true
	default:
		return false, false
	}
}

func setSingleActiveConsumerLinkStateHandler(opts *amqp.ReceiverOptions, options IConsumerOptions, c *Consumer) {
	if opts == nil || c == nil || options == nil {
		return
	}
	co, ok := options.(*ConsumerOptions)
	if !ok || co.SingleActiveConsumerStateChanged == nil {
		return
	}
	handler := co.SingleActiveConsumerStateChanged
	opts.OnLinkStateProperties = func(props map[string]any) {
		if len(props) == 0 {
			return
		}
		v, has := props[rabbitmqActiveLinkStateProperty]
		if !has {
			return
		}
		active, parsed := rabbitmqActiveFromLinkState(v)
		if !parsed {
			Warn("Ignoring rabbitmq:active with unsupported type", "value", v)
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					Error("SingleActiveConsumerStateChanged handler panicked", "recover", r)
				}
			}()
			handler(c, active)
		}()
	}
}

func newConsumer(ctx context.Context, connection *AmqpConnection, destinationAdd string, options IConsumerOptions) (*Consumer, error) {
	id := fmt.Sprintf("consumer-%s", uuid.New().String())
	if options != nil && options.id() != "" {
		id = options.id()
	}

	r := &Consumer{connection: connection, options: options,
		destinationAdd: destinationAdd,
		id:             id}
	r.currentOffset.Store(-1)
	connection.entitiesTracker.storeOrReplaceConsumer(r)
	err := r.createReceiver(ctx)
	if err != nil {
		return nil, err
	}

	// Record the consumer opening metric
	connection.metricsCollector.OpenConsumer()

	return r, nil
}

func (c *Consumer) createReceiver(ctx context.Context) error {
	c.optionsMu.Lock()
	defer c.optionsMu.Unlock()

	offset := c.currentOffset.Load()
	if offset >= 0 {
		// here it means that the consumer is a stream consumer and there is a restart.
		// so we need to set the offset to the last consumed message in order to restart from there.
		// If there is not a restart this code won't be executed.
		if c.options != nil {
			// here we assume it is a stream. So we recreate the options with the offset.
			streamOpts := &StreamConsumerOptions{
				ReceiverLinkName: c.options.linkName(),
				InitialCredits:   c.options.initialCredits(),
				// we increment the offset by one to start from the next message.
				// because the current was already consumed.
				Offset: &OffsetValue{Offset: uint64(offset + 1)},
			}
			// Preserve StreamFilterOptions if it's a StreamConsumerOptions
			if sco, ok := c.options.(*StreamConsumerOptions); ok {
				streamOpts.StreamFilterOptions = sco.StreamFilterOptions
			}
			c.options = streamOpts
		}
	}
	// define a variable  *amqp.ReceiverOptions type
	var receiverOptions *amqp.ReceiverOptions

	// by default, we create a normal receiver link
	// but if direct-reply-to is enabled, we create a dynamic receiver link
	if c.options != nil && c.options.isDirectReplyToEnable() {
		receiverOptions = createDynamicReceiverLinkOptions(c.options)
	} else {
		// normal receiver link, inside createReceiverLinkOptions we check if pre-settled mode is enabled
		// so, by default we use AtLeastOnce settlement mode even is not specified
		receiverOptions = createReceiverLinkOptions(c.destinationAdd, c.options, AtLeastOnce)
		setSingleActiveConsumerLinkStateHandler(receiverOptions, c.options, c)
	}

	receiver, err := c.connection.session.NewReceiver(ctx, c.destinationAdd, receiverOptions)
	if err != nil {
		return err
	}

	c.queue = receiver.Address()

	c.receiver.Swap(receiver)
	return nil
}

// Receive blocks until a message is available on the link or the context is cancelled.
// It returns an IDeliveryContext that must be settled by the caller (Accept, Discard, or Requeue)
// unless the consumer was created with PreSettled settle strategy, in which case the broker
// has already settled the delivery and calling any settlement method returns ErrPreSettledMessageDisposed.
func (c *Consumer) Receive(ctx context.Context) (IDeliveryContext, error) {
	msg, err := c.receiver.Load().Receive(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Build consume context for OTEL semantic convention attributes
	consumeCtx := c.buildConsumeContext(msg)

	// Record the consume metric
	c.connection.metricsCollector.Consume(consumeCtx)

	if msg != nil && msg.Annotations != nil && msg.Annotations["x-stream-offset"] != nil {
		if off, ok := streamOffsetFromAnnotation(msg.Annotations["x-stream-offset"]); ok {
			c.currentOffset.Store(off)
		} else {
			Debug("Ignoring x-stream-offset with unsupported type", "value", msg.Annotations["x-stream-offset"])
		}
	}

	// Check if pre-settled mode is enabled
	c.optionsMu.RLock()
	preSettled := c.options != nil && c.options.preSettled()
	c.optionsMu.RUnlock()
	if preSettled {
		// For pre-settled mode, immediately record the disposition as accepted
		// since the message is already settled by the broker
		c.connection.metricsCollector.ConsumeDisposition(ConsumeAccepted, consumeCtx)
		return &PreSettledDeliveryContext{message: msg}, nil
	}

	return &DeliveryContext{
		receiver:         c.receiver.Load(),
		message:          msg,
		metricsCollector: c.connection.metricsCollector,
		consumeCtx:       consumeCtx,
	}, nil
}

// Close detaches the consumer link and removes the consumer from the connection's
// entity tracker. After Close returns, no further calls to Receive should be made.
func (c *Consumer) Close(ctx context.Context) error {
	c.connection.entitiesTracker.removeConsumer(c)
	err := c.receiver.Load().Close(ctx)

	// Record the consumer closing metric
	c.connection.metricsCollector.CloseConsumer()

	return err
}

// GetQueue returns the queue the consumer is connected to.
// When the user sets the destination address to a dynamic address, this function will return the dynamic address.
// like direct-reply-to address. In other cases, it will return the queue address.
func (c *Consumer) GetQueue() (string, error) {
	return parseQueueAddress(c.queue)
}

// pause drains the credits of the receiver and stops issuing new credits.
func (c *Consumer) pause(ctx context.Context) error {
	for {
		s := c.state.Load()
		if s == uint32(consumerStatePaused) || s == uint32(consumerStatePausing) {
			return nil
		}
		if c.state.CompareAndSwap(uint32(consumerStateRunning), uint32(consumerStatePausing)) {
			break
		}
	}
	err := c.receiver.Load().DrainCredit(ctx, nil)
	if err != nil {
		c.state.Store(uint32(consumerStateRunning))
		return fmt.Errorf("error draining credits: %w", err)
	}
	c.state.Store(uint32(consumerStatePaused))
	return nil
}

// unpause requests new credits using the initial credits value of the options.
func (c *Consumer) unpause(credits uint32) error {
	if c.state.Load() == uint32(consumerStateRunning) {
		return nil
	}
	err := c.receiver.Load().IssueCredit(credits)
	if err != nil {
		return fmt.Errorf("error issuing credits: %w", err)
	}
	c.state.Store(uint32(consumerStateRunning))
	return nil
}

func (c *Consumer) isPausedOrPausing() bool {
	return c.state.Load() != uint32(consumerStateRunning)
}

// issueCredits issues more credits on the receiver.
func (c *Consumer) issueCredits(credits uint32) error {
	return c.receiver.Load().IssueCredit(credits)
}

// buildConsumeContext builds a ConsumeContext from the consumer and message.
func (c *Consumer) buildConsumeContext(message *amqp.Message) ConsumeContext {
	// Parse the queue name from the destination address
	queueName, err := c.GetQueue()
	if err != nil {
		Debug("Could not parse queue address for consume metrics", "error", err, "queue", c.queue)
	}

	var messageID string
	if message != nil && message.Properties != nil && message.Properties.MessageID != nil {
		// MessageID can be various types, convert to string
		messageID = fmt.Sprintf("%v", message.Properties.MessageID)
	}

	return ConsumeContext{
		ServerAddress:   c.connection.serverAddress,
		ServerPort:      c.connection.serverPort,
		DestinationName: queueName,
		MessageID:       messageID,
	}
}
