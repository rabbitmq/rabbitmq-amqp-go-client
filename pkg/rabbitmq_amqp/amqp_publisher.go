package rabbitmq_amqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"sync/atomic"
)

type PublishResult struct {
	Outcome amqp.DeliveryState
	Message *amqp.Message
}

// Publisher is a publisher that sends messages to a specific destination address.
type Publisher struct {
	sender         atomic.Pointer[amqp.Sender]
	connection     *AmqpConnection
	linkName       string
	destinationAdd string
	id             string
}

func (m *Publisher) Id() string {
	return m.id
}

func newPublisher(ctx context.Context, connection *AmqpConnection, destinationAdd string, linkName string, args ...string) (*Publisher, error) {
	id := fmt.Sprintf("publisher-%s", uuid.New().String())
	if len(args) > 0 {
		id = args[0]
	}

	r := &Publisher{connection: connection, linkName: linkName, destinationAdd: destinationAdd, id: id}
	connection.entitiesTracker.storeOrReplaceProducer(r)
	err := r.createSender(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (m *Publisher) createSender(ctx context.Context) error {
	sender, err := m.connection.session.NewSender(ctx, m.destinationAdd, createSenderLinkOptions(m.destinationAdd, m.linkName, AtLeastOnce))
	if err != nil {
		return err
	}
	m.sender.Store(sender)
	return nil
}

/*
Publish sends a message to the destination address that can be decided during the creation of the publisher or at the time of sending the message.

The message is sent and the outcome of the operation is returned.
The outcome is a DeliveryState that indicates if the message was accepted or rejected.
RabbitMQ supports the following DeliveryState types:

  - StateAccepted
  - StateReleased
  - StateRejected
    See: https://www.rabbitmq.com/docs/next/amqp#outcomes for more information.

Note: If the destination address is not defined during the creation, the message must have a TO property set.
You can use the helper "MessageToAddressHelper" to create the destination address.
See the examples:
Create a new publisher that sends messages to a specific destination address:
<code>

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rabbitmq_amqp.ExchangeAddress{
				Exchange: "myExchangeName",
				Key:      "myRoutingKey",
			}

	.. publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!")))

</code>
Create a new publisher that sends messages based on message destination address:
<code>

	publisher, err := connection.NewPublisher(context.Background(), nil, "test")
	msg := amqp.NewMessage([]byte("hello"))
	..:= MessageToAddressHelper(msg, &QueueAddress{Queue: "myQueueName"})
	..:= publisher.Publish(context.Background(), msg)

</code>
*/
func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) (*PublishResult, error) {
	if m.destinationAdd == "" {
		if message.Properties == nil || message.Properties.To == nil {
			return nil, fmt.Errorf("message properties TO is required to send a message to a dynamic target address")
		}

		err := validateAddress(*message.Properties.To)
		if err != nil {
			return nil, err
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
	publishResult := &PublishResult{
		Message: message,
		Outcome: state,
	}
	return publishResult, err
}

// Close closes the publisher.
func (m *Publisher) Close(ctx context.Context) error {
	m.connection.entitiesTracker.removeProducer(m)
	return m.sender.Load().Close(ctx)
}
