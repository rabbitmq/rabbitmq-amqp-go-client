package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

type PublishResult struct {
	Outcome DeliveryState
	Message *amqp.Message
}

// Publisher is a publisher that sends messages to a specific destination address.
type Publisher struct {
	sender             atomic.Pointer[amqp.Sender]
	connection         *AmqpConnection
	linkName           string
	destinationAddress string
	id                 string
}

func (m *Publisher) ID() string {
	return m.id
}

func newPublisher(ctx context.Context, connection *AmqpConnection, destinationAddress string, options PublisherOptions) (*Publisher, error) {
	id := fmt.Sprintf("publisher-%s", uuid.New().String())
	if options != nil && options.ID() != "" {
		id = options.ID()
	}

	linkName := ""
	if options != nil {
		linkName = options.LinkName()
	}
	r := &Publisher{connection: connection, linkName: getLinkName(linkName), destinationAddress: destinationAddress, id: id}
	connection.entitiesTracker.storeOrReplaceProducer(r)
	err := r.createSender(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (m *Publisher) createSender(ctx context.Context) error {
	sender, err := m.connection.session.NewSender(ctx, m.destinationAddress, createSenderLinkOptions(m.destinationAddress, m.linkName, AtLeastOnce))
	if err != nil {
		return err
	}
	m.sender.Swap(sender)
	return nil
}

// Publish sends a message and returns the delivery outcome.
// Messages are persistent by default. Set Header.Durable to false for non-persistent messages.
// If no destination was set during publisher creation, the message must have a TO property.
func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) (*PublishResult, error) {
	if m.destinationAddress == "" {
		if message.Properties == nil || message.Properties.To == nil {
			return nil, fmt.Errorf("message properties TO is required to send a message to a dynamic target address")
		}

		err := validateAddress(*message.Properties.To)
		if err != nil {
			return nil, err
		}
	}

	// set the default persistence to the message
	if message.Header == nil {
		message.Header = &amqp.MessageHeader{
			Durable: true,
		}
	}

	r, err := m.sender.Load().SendWithReceipt(ctx, message, nil)
	if err != nil {
		return nil, err
	}
	state, err := r.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return &PublishResult{
		Message: message,
		Outcome: state,
	}, err
}

// Close closes the publisher.
func (m *Publisher) Close(ctx context.Context) error {
	m.connection.entitiesTracker.removeProducer(m)
	return m.sender.Load().Close(ctx)
}
