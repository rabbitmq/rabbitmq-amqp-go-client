package rabbitmqamqp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
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
	sender         atomic.Pointer[amqp.Sender]
	connection     *AmqpConnection
	linkName       string
	destinationAdd string
	id             string
}

func (m *Publisher) Id() string {
	return m.id
}

func newPublisher(ctx context.Context, connection *AmqpConnection, destinationAdd string, options IPublisherOptions) (*Publisher, error) {
	id := fmt.Sprintf("publisher-%s", uuid.New().String())
	if options != nil && options.id() != "" {
		id = options.id()
	}

	r := &Publisher{connection: connection, linkName: getLinkName(options), destinationAdd: destinationAdd, id: id}
	connection.entitiesTracker.storeOrReplaceProducer(r)
	err := r.createSender(ctx)
	if err != nil {
		return nil, err
	}

	// Record the publisher opening metric
	connection.metricsCollector.OpenPublisher()

	return r, nil
}

func (m *Publisher) createSender(ctx context.Context) error {
	sender, err := m.connection.session.NewSender(ctx, m.destinationAdd, createSenderLinkOptions(m.destinationAdd, m.linkName, AtLeastOnce))
	if err != nil {
		return err
	}
	m.sender.Swap(sender)
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

If the destination address is not defined during the creation, the message must have a TO property set.
You can use the helper "MessagePropertyToAddress" to create the destination address.
See the examples:
Create a new publisher that sends messages to a specific destination address:
<code>

	publisher, err := amqpConnection.NewPublisher(context.Background(), &rabbitmqamqp.ExchangeAddress{
				Exchange: "myExchangeName",
				Key:      "myRoutingKey",
			}

	.. publisher.Publish(context.Background(), amqp.NewMessage([]byte("Hello, World!")))

</code>
Create a new publisher that sends messages based on message destination address:
<code>

	publisher, err := connection.NewPublisher(context.Background(), nil, "test")
	msg := amqp.NewMessage([]byte("hello"))
	..:= MessagePropertyToAddress(msg, &QueueAddress{Queue: "myQueueName"})
	..:= publisher.Publish(context.Background(), msg)

</code>

The message is persistent by default by setting the Header.Durable to true when Header is nil.
You can set the message to be non-persistent by setting the Header.Durable to false.
Note:
When you use the `Header` is up to you to set the message properties,
You need set the `Header.Durable` to true or false.

<code>

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

	// Build publish context for OTEL semantic convention attributes
	publishCtx := m.buildPublishContext(message)

	// Record the publish metric immediately after sending
	m.connection.metricsCollector.Publish(publishCtx)

	state, err := r.Wait(ctx)
	if err != nil {
		return nil, err
	}

	// Record the publish disposition metric based on the outcome
	m.recordPublishDisposition(state, publishCtx)

	return &PublishResult{
		Message: message,
		Outcome: state,
	}, err
}

// recordPublishDisposition records the publish disposition metric based on the delivery state.
func (m *Publisher) recordPublishDisposition(state DeliveryState, ctx PublishContext) {
	switch state.(type) {
	case *StateAccepted:
		m.connection.metricsCollector.PublishDisposition(PublishAccepted, ctx)
	case *StateRejected:
		m.connection.metricsCollector.PublishDisposition(PublishRejected, ctx)
	case *StateReleased:
		m.connection.metricsCollector.PublishDisposition(PublishReleased, ctx)
	}
}

// buildPublishContext builds a PublishContext from the publisher and message.
func (m *Publisher) buildPublishContext(message *amqp.Message) PublishContext {
	destinationName, routingKey := m.parseDestination()

	var messageID string
	if message.Properties != nil && message.Properties.MessageID != nil {
		// MessageID can be various types, convert to string
		messageID = fmt.Sprintf("%v", message.Properties.MessageID)
	}

	return PublishContext{
		ServerAddress:   m.connection.serverAddress,
		ServerPort:      m.connection.serverPort,
		DestinationName: destinationName,
		RoutingKey:      routingKey,
		MessageID:       messageID,
	}
}

// parseDestination parses the destination address and returns the destination name and routing key.
// The destination name follows OTEL semantic conventions:
// - For exchanges: "{exchange}:{routing_key}" or just "{exchange}"
// - For queues: "{queue}"
func (m *Publisher) parseDestination() (destinationName, routingKey string) {
	address := m.destinationAdd
	if address == "" {
		return "", ""
	}

	// Address format: /exchanges/{exchange}/{key} or /queues/{queue}
	if strings.HasPrefix(address, "/"+exchanges+"/") {
		// Exchange address
		path := strings.TrimPrefix(address, "/"+exchanges+"/")
		parts := strings.SplitN(path, "/", 2)
		exchange, _ := url.QueryUnescape(parts[0])
		if len(parts) > 1 {
			routingKey, _ = url.QueryUnescape(parts[1])
			destinationName = buildDestinationName(exchange, routingKey, "")
		} else {
			destinationName = exchange
		}
	} else if strings.HasPrefix(address, "/"+queues+"/") {
		// Queue address
		queue := strings.TrimPrefix(address, "/"+queues+"/")
		queue, _ = url.QueryUnescape(queue)
		destinationName = queue
	}

	return destinationName, routingKey
}

// Close closes the publisher.
func (m *Publisher) Close(ctx context.Context) error {
	m.connection.entitiesTracker.removeProducer(m)
	err := m.sender.Load().Close(ctx)

	// Record the publisher closing metric
	m.connection.metricsCollector.ClosePublisher()

	return err
}
