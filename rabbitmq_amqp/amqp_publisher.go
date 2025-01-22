package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type PublishResult struct {
	Outcome amqp.DeliveryState
	Message *amqp.Message
}

type Publisher struct {
	sender *amqp.Sender
}

func newPublisher(sender *amqp.Sender) *Publisher {
	return &Publisher{sender: sender}
}

// Publish sends a message to the destination address.
// The message is sent to the destination address and the outcome of the operation is returned.
// The outcome is a DeliveryState that indicates if the message was accepted or rejected.
// RabbitMQ supports the following DeliveryState types:
// - StateAccepted
// - StateReleased
// - StateRejected
// See: https://www.rabbitmq.com/docs/next/amqp#outcomes for more information.
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
		Message: message,
		Outcome: state,
	}
	return publishResult, err
}

// Close closes the publisher.
func (m *Publisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
