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

func (c *Consumer) AcceptMessage(ctx context.Context, message *amqp.Message) error {
	return c.receiver.AcceptMessage(ctx, message)
}

func (c *Consumer) RejectMessage(ctx context.Context, message *amqp.Message, e *amqp.Error) error {
	return c.receiver.RejectMessage(ctx, message, e)
}

func (c *Consumer) ReleaseMessage(ctx context.Context, message *amqp.Message) error {
	return c.receiver.ReleaseMessage(ctx, message)
}

func (c *Consumer) Close(ctx context.Context) error {
	return c.receiver.Close(ctx)
}
