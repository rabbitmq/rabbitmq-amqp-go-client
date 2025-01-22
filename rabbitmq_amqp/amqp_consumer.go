package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
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
	for key, value := range annotations {
		destination[key] = value

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
	receiver *amqp.Receiver
}

func newConsumer(receiver *amqp.Receiver) *Consumer {
	return &Consumer{receiver: receiver}
}

func (c *Consumer) Receive(ctx context.Context) (*DeliveryContext, error) {
	msg, err := c.receiver.Receive(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &DeliveryContext{receiver: c.receiver, message: msg}, nil
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.receiver.Close(ctx)
}
