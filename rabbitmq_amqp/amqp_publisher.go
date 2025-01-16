package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type PublishOutcome struct {
	DeliveryState amqp.DeliveryState
}

type Publisher struct {
	sender *amqp.Sender
}

func newPublisher(sender *amqp.Sender) *Publisher {
	return &Publisher{sender: sender}
}

func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) (*PublishOutcome, error) {

	/// for the outcome of the message delivery, see https://github.com/Azure/go-amqp/issues/347
	//RELEASED
	///**
	// * The broker could not route the message to any queue.
	// *
	// * <p>This is likely to be due to a topology misconfiguration.
	// */
	// so at the moment we don't have access on this information
	// TODO: remove this comment when the issue is resolved
	outcome := &PublishOutcome{}
	r, err := m.sender.SendWithReceipt(ctx, message, nil)
	if err != nil {
		return nil, err
	}
	state, err := r.Wait(ctx)
	if err != nil {
		return nil, err
	}

	outcome.DeliveryState = state

	return outcome, err
}

func (m *Publisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
