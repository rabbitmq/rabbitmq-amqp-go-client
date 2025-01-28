package rabbitmq_amqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
)

type PublishResult struct {
	Outcome amqp.DeliveryState
	Message *amqp.Message
}

// TargetPublisher is a publisher that sends messages to a specific destination address.
// The destination address is decided during the creation of the publisher.
type TargetPublisher struct {
	sender *amqp.Sender
}

func newTargetPublisher(sender *amqp.Sender) *TargetPublisher {
	return &TargetPublisher{sender: sender}
}

// Publish sends a message to the destination address decided during the creation of the publisher.
// The message is sent and the outcome of the operation is returned.
// The outcome is a DeliveryState that indicates if the message was accepted or rejected.
// RabbitMQ supports the following DeliveryState types:
// - StateAccepted
// - StateReleased
// - StateRejected
// See: https://www.rabbitmq.com/docs/next/amqp#outcomes for more information.
func (m *TargetPublisher) Publish(ctx context.Context, message *amqp.Message) (*PublishResult, error) {
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
func (m *TargetPublisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}

type TargetsPublisher struct {
	sender *amqp.Sender
}

func newTargetsPublisher(sender *amqp.Sender) *TargetsPublisher {
	return &TargetsPublisher{sender: sender}
}

// Publish sends a message to the destination address for each send.
// The message is sent and the outcome of the operation is returned.
// destinationAdd will be used as the To field in the message properties.
// The outcome is a DeliveryState that indicates if the message was accepted or rejected.
// RabbitMQ supports the following DeliveryState types:
// - StateAccepted
// - StateReleased
// - StateRejected
// See: https://www.rabbitmq.com/docs/next/amqp#outcomes for more information.
func (m *TargetsPublisher) Publish(ctx context.Context, message *amqp.Message, destination TargetAddress) (*PublishResult, error) {
	destinationAdd, err := destination.toAddress()
	if err != nil {
		return nil, err
	}

	if !validateAddress(destinationAdd) {
		return nil, fmt.Errorf("invalid destination address, the address should start with /%s/ or/%s/ ", exchanges, queues)
	}

	if message.Properties == nil {
		message.Properties = &amqp.MessageProperties{}
	}
	message.Properties.To = &destinationAdd

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
func (m *TargetsPublisher) Close(ctx context.Context) error {
	return m.sender.Close(ctx)
}
