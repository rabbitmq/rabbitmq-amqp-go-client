package rabbitmq_amqp

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
	linkName       string
	destinationAdd string
	id             string
}

func (c *Consumer) Id() string {
	return c.id
}

func newConsumer(ctx context.Context, connection *AmqpConnection, destinationAdd string, linkName string, args ...string) (*Consumer, error) {
	id := fmt.Sprintf("consumer-%s", uuid.New().String())
	if len(args) > 0 {
		id = args[0]
	}
	r := &Consumer{connection: connection, linkName: linkName, destinationAdd: destinationAdd, id: id}
	connection.entitiesTracker.storeOrReplaceConsumer(r)
	err := r.createReceiver(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (c *Consumer) createReceiver(ctx context.Context) error {
	receiver, err := c.connection.session.NewReceiver(ctx, c.destinationAdd, createReceiverLinkOptions(c.destinationAdd, c.linkName, AtLeastOnce))
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
	return &DeliveryContext{receiver: c.receiver.Load(), message: msg}, nil
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.receiver.Load().Close(ctx)
}
