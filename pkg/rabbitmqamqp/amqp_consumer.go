package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync/atomic"

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
	RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error
}

type DeliveryContext struct {
	receiver         *amqp.Receiver
	message          *amqp.Message
	metricsCollector MetricsCollector
	consumeCtx       ConsumeContext // For OTEL semantic convention attributes
}

func (dc *DeliveryContext) Message() *amqp.Message {
	return dc.message
}

func (dc *DeliveryContext) Accept(ctx context.Context) error {
	err := dc.receiver.AcceptMessage(ctx, dc.message)
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeAccepted, dc.consumeCtx)
	}
	return err
}

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

func (dc *DeliveryContext) Requeue(ctx context.Context) error {
	err := dc.receiver.ReleaseMessage(ctx, dc.message)
	if err == nil {
		dc.metricsCollector.ConsumeDisposition(ConsumeRequeued, dc.consumeCtx)
	}
	return err
}

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

// PreSettledDeliveryContext represents a delivery context for pre-settled messages.
// All settlement methods throw errors since the message is already settled.
type PreSettledDeliveryContext struct {
	message *amqp.Message
}

func (dc *PreSettledDeliveryContext) Message() *amqp.Message {
	return dc.message
}

func (dc *PreSettledDeliveryContext) Accept(ctx context.Context) error {
	return fmt.Errorf("auto-settle on, message is already disposed")
}

func (dc *PreSettledDeliveryContext) Discard(ctx context.Context, e *amqp.Error) error {
	return fmt.Errorf("auto-settle on, message is already disposed")
}

func (dc *PreSettledDeliveryContext) DiscardWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	return fmt.Errorf("auto-settle on, message is already disposed")
}

func (dc *PreSettledDeliveryContext) Requeue(ctx context.Context) error {
	return fmt.Errorf("auto-settle on, message is already disposed")
}

func (dc *PreSettledDeliveryContext) RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	return fmt.Errorf("auto-settle on, message is already disposed")
}

type consumerState byte

const (
	consumerStateRunning consumerState = iota
	consumerStatePausing
	consumerStatePaused
)

type Consumer struct {
	receiver       atomic.Pointer[amqp.Receiver]
	connection     *AmqpConnection
	options        IConsumerOptions
	destinationAdd string
	id             string

	/*
		currentOffset is the current offset of the consumer. It is valid only for the stream consumers.
		it is used to keep track of the last message that was consumed by the consumer.
		so in case of restart the consumer can start from the last message that was consumed.
		For the AMQP queues it is just ignored.
	*/
	currentOffset int64

	state consumerState

	// see GetQueue method for more details.
	queue string
}

func (c *Consumer) Id() string {
	return c.id
}

func newConsumer(ctx context.Context, connection *AmqpConnection, destinationAdd string, options IConsumerOptions) (*Consumer, error) {
	id := fmt.Sprintf("consumer-%s", uuid.New().String())
	if options != nil && len(options.id()) > 0 {
		id = options.id()
	}

	r := &Consumer{connection: connection, options: options,
		destinationAdd: destinationAdd,
		currentOffset:  -1,
		id:             id}
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
	if c.currentOffset >= 0 {
		// here it means that the consumer is a stream consumer and there is a restart.
		// so we need to set the offset to the last consumed message in order to restart from there.
		// In there is not a restart this code won't be executed.
		if c.options != nil {
			// here we assume it is a stream. So we recreate the options with the offset.
			streamOpts := &StreamConsumerOptions{
				ReceiverLinkName: c.options.linkName(),
				InitialCredits:   c.options.initialCredits(),
				// we increment the offset by one to start from the next message.
				// because the current was already consumed.
				Offset: &OffsetValue{Offset: uint64(c.currentOffset + 1)},
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
	}

	receiver, err := c.connection.session.NewReceiver(ctx, c.destinationAdd, receiverOptions)
	if err != nil {
		return err
	}

	c.queue = receiver.Address()

	c.receiver.Swap(receiver)
	return nil
}

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
		// keep track of the current offset of the consumer
		c.currentOffset = msg.Annotations["x-stream-offset"].(int64)
	}

	// Check if pre-settled mode is enabled
	if c.options != nil && c.options.preSettled() {
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
	if c.state == consumerStatePaused || c.state == consumerStatePausing {
		return nil
	}
	c.state = consumerStatePausing
	err := c.receiver.Load().DrainCredit(ctx, nil)
	if err != nil {
		c.state = consumerStateRunning
		return fmt.Errorf("error draining credits: %w", err)
	}
	c.state = consumerStatePaused
	return nil
}

// unpause requests new credits using the initial credits value of the options.
func (c *Consumer) unpause(credits uint32) error {
	if c.state == consumerStateRunning {
		return nil
	}
	err := c.receiver.Load().IssueCredit(credits)
	if err != nil {
		return fmt.Errorf("error issuing credits: %w", err)
	}
	c.state = consumerStateRunning
	return nil
}

func (c *Consumer) isPausedOrPausing() bool {
	return c.state != consumerStateRunning
}

// issueCredits issues more credits on the receiver.
func (c *Consumer) issueCredits(credits uint32) error {
	return c.receiver.Load().IssueCredit(credits)
}

// buildConsumeContext builds a ConsumeContext from the consumer and message.
func (c *Consumer) buildConsumeContext(message *amqp.Message) ConsumeContext {
	// Parse the queue name from the destination address
	queueName, _ := c.GetQueue()

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
