package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type MPublisher struct {
	sender *amqp.Sender
}

func NewMPublisher(sender *amqp.Sender) *MPublisher {
	return &MPublisher{sender: sender}
}

func (m *MPublisher) Publish(ctx context.Context, message *amqp.Message, address *AddressBuilder) error {

	messageTo, err := address.Address()
	if err != nil {
		return err
	}
	if message.Properties == nil {
		message.Properties = &amqp.MessageProperties{}
	}
	message.Properties.To = &messageTo
	err = m.sender.Send(ctx, message, nil)
	if err != nil {
		return err
	}
	return nil
}

func (m *MPublisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
