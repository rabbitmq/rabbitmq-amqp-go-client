package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type PublishResult struct {
	DeliveryState amqp.DeliveryState
	Message       *amqp.Message
}

type Publisher struct {
	sender *amqp.Sender
}

func newPublisher(sender *amqp.Sender) *Publisher {
	return &Publisher{sender: sender}
}

func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) (*PublishResult, error) {

	r, err := m.sender.SendWithReceipt(ctx, message, nil)
	if err != nil {
		return nil, err
	}
	state, err := r.Wait(ctx)
	if err != nil {
		return nil, err
	}

	publishResult := &PublishResult{
		Message:       message,
		DeliveryState: state,
	}
	return publishResult, err
}

func (m *Publisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
