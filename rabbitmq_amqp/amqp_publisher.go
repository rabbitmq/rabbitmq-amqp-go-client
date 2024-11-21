package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type Publisher struct {
	sender *amqp.Sender
}

func newPublisher(sender *amqp.Sender) *Publisher {
	return &Publisher{sender: sender}
}

func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) error {

	/// for the outcome of the message delivery, see https://github.com/Azure/go-amqp/issues/347
	//RELEASED
	///**
	// * The broker could not route the message to any queue.
	// *
	// * <p>This is likely to be due to a topology misconfiguration.
	// */
	// so at the moment we don't have access on this information
	// TODO: remove this comment when the issue is resolved

	err := m.sender.Send(ctx, message, nil)

	if err != nil {
		return err
	}
	return nil
}

func (m *Publisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
