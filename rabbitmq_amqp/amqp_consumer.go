package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type Consumer struct {
	receiver *amqp.Receiver
}

func newConsumer(receiver *amqp.Receiver) *Consumer {
	return &Consumer{receiver: receiver}
}

func (c *Consumer) Receive(ctx context.Context) (*amqp.Message, error) {
	return c.receiver.Receive(ctx, nil)
}

func (c *Consumer) Accept(ctx context.Context, message *amqp.Message) error {
	return c.receiver.AcceptMessage(ctx, message)
}

func (c *Consumer) Discard(ctx context.Context, message *amqp.Message, e *amqp.Error) error {
	return c.receiver.RejectMessage(ctx, message, e)
}

func (c *Consumer) DiscardWithAnnotations(ctx context.Context, message *amqp.Message, annotations Annotations) error {
	if err := validateMessageAnnotations(annotations); err != nil {
		return err
	}
	// copy the rabbitmq annotations  to amqp annotations
	destination := make(amqp.Annotations)
	for key, value := range annotations {
		destination[key] = value

	}

	return c.receiver.ModifyMessage(ctx, message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    true,
		UndeliverableHere: true,
		Annotations:       destination,
	})
}

func (c *Consumer) Requeue(ctx context.Context, message *amqp.Message) error {
	return c.receiver.ReleaseMessage(ctx, message)
}

func (c *Consumer) RequeueWithAnnotations(ctx context.Context, message *amqp.Message, annotations Annotations) error {
	if err := validateMessageAnnotations(annotations); err != nil {
		return err
	}
	// copy the rabbitmq annotations  to amqp annotations
	destination := make(amqp.Annotations)
	for key, value := range annotations {
		destination[key] = value

	}
	return c.receiver.ModifyMessage(ctx, message, &amqp.ModifyMessageOptions{
		DeliveryFailed:    false,
		UndeliverableHere: false,
		Annotations:       destination,
	})
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.receiver.Close(ctx)
}
