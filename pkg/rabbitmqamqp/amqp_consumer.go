package rabbitmqamqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"sync/atomic"
)

type DeliveryContext struct {
	receiver *amqp.Receiver
	message  *amqp.Message
}

func (dc *DeliveryContext) Message() *amqp.Message {
	return dc.message
}

func (dc *DeliveryContext) Accept(ctx context.Context) error {
	return dc.receiver.AcceptMessage(ctx, dc.message)
}

func (dc *DeliveryContext) Discard(ctx context.Context, e *amqp.Error) error {
	return dc.receiver.RejectMessage(ctx, dc.message, e)
}

func (dc *DeliveryContext) DiscardWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	if err := validateMessageAnnotations(annotations); err != nil {
		return err
	}
	// copy the rabbitmq annotations  to amqp annotations
	destination := make(amqp.Annotations)
	for keyA, value := range annotations {
		destination[keyA] = value

	}

	return dc.receiver.ModifyMessage(ctx, dc.message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    true,
		UndeliverableHere: true,
		Annotations:       destination,
	})
}

func (dc *DeliveryContext) Requeue(ctx context.Context) error {
	return dc.receiver.ReleaseMessage(ctx, dc.message)
}

func (dc *DeliveryContext) RequeueWithAnnotations(ctx context.Context, annotations amqp.Annotations) error {
	if err := validateMessageAnnotations(annotations); err != nil {
		return err
	}
	// copy the rabbitmq annotations  to amqp annotations
	destination := make(amqp.Annotations)
	for key, value := range annotations {
		destination[key] = value

	}
	return dc.receiver.ModifyMessage(ctx, dc.message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    false,
		UndeliverableHere: false,
		Annotations:       destination,
	})
}

type Consumer struct {
	receiver       atomic.Pointer[amqp.Receiver]
	connection     *AmqpConnection
	options        ConsumerOptions
	destinationAdd string
	id             string

	/*
		currentOffset is the current offset of the consumer. It is valid only for the stream consumers.
		it is used to keep track of the last message that was consumed by the consumer.
		so in case of restart the consumer can start from the last message that was consumed.
		For the AMQP queues it is just ignored.
	*/
	currentOffset int64
}

func (c *Consumer) Id() string {
	return c.id
}

func newConsumer(ctx context.Context, connection *AmqpConnection, destinationAdd string, options ConsumerOptions, args ...string) (*Consumer, error) {
	id := fmt.Sprintf("consumer-%s", uuid.New().String())
	if len(args) > 0 {
		id = args[0]
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
	return r, nil
}

func (c *Consumer) createReceiver(ctx context.Context) error {
	if c.currentOffset >= 0 {
		// here it means that the consumer is a stream consumer and there is a restart.
		// so we need to set the offset to the last consumed message in order to restart from there.
		// In there is not a restart this code won't be executed.
		if c.options != nil {
			// here we assume it is a stream. So we recreate the options with the offset.
			c.options = &StreamConsumerOptions{
				ReceiverLinkName: c.options.linkName(),
				InitialCredits:   c.options.initialCredits(),
				// we increment the offset by one to start from the next message.
				// because the current was already consumed.
				Offset: &OffsetValue{Offset: uint64(c.currentOffset + 1)},
			}
		}
	}

	receiver, err := c.connection.session.NewReceiver(ctx, c.destinationAdd,
		createReceiverLinkOptions(c.destinationAdd, c.options, AtLeastOnce))
	if err != nil {
		return err
	}

	c.receiver.Swap(receiver)
	return nil
}

func (c *Consumer) Receive(ctx context.Context) (*DeliveryContext, error) {
	msg, err := c.receiver.Load().Receive(ctx, nil)
	if err != nil {
		return nil, err
	}

	if msg != nil && msg.Annotations != nil && msg.Annotations["x-stream-offset"] != nil {
		// keep track of the current offset of the consumer
		c.currentOffset = msg.Annotations["x-stream-offset"].(int64)
	}

	return &DeliveryContext{receiver: c.receiver.Load(), message: msg}, nil
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.receiver.Load().Close(ctx)
}
