package rabbitmqamqp

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// MessageRejectedError contains details about why a message was rejected by the broker.
// When RabbitMQ 4.3+ rejects a message (e.g. because a queue's max-length has been
// reached), the broker includes the name of the queue that rejected it and a
// human-readable reason inside the AMQP 1.0 Rejected outcome.
//
// See: https://www.rabbitmq.com/blog/2026/04/23/rabbitmq-4.3-release#amqp-rejection-reason
type MessageRejectedError struct {
	// RejectedBy is the name of the queue that rejected the message.
	// Empty when the broker did not include this information.
	RejectedBy string

	// Reason is the human-readable description of why the message was rejected
	// (e.g. "maximum length reached", "queue unavailable").
	Reason string
}

func (e *MessageRejectedError) Error() string {
	if e.RejectedBy != "" {
		return fmt.Sprintf("message rejected by queue '%s': %s", e.RejectedBy, e.Reason)
	}
	return fmt.Sprintf("message rejected: %s", e.Reason)
}

// PublishResult is returned by Publish and the PublishAsync callback.
// When Outcome is *StateRejected and the broker provided rejection details
// (requires RabbitMQ 4.3+), MessageRejectedError is non-nil and carries the
// name of the rejecting queue and the rejection reason.
type PublishResult struct {
	Outcome DeliveryState
	Message *amqp.Message
	// MessageRejectedError is non-nil when Outcome is *StateRejected and the
	// broker supplied rejection details (RabbitMQ 4.3+).
	MessageRejectedError *MessageRejectedError
}

// extractMessageRejectedError extracts a MessageRejectedError from a DeliveryState.
// Returns nil when state is not *StateRejected or the Rejected outcome carries no Error.
// The queue name is read from StateRejected.Error.Info["queue"];
// the rejection reason comes from StateRejected.Error.Description.
func extractMessageRejectedError(state DeliveryState) *MessageRejectedError {
	rejected, ok := state.(*StateRejected)
	if !ok || rejected.Error == nil {
		return nil
	}
	result := &MessageRejectedError{
		Reason: rejected.Error.Description,
	}
	if rejected.Error.Info != nil {
		if v, exists := rejected.Error.Info["queue"]; exists {
			if s, ok := v.(string); ok {
				result.RejectedBy = s
			}
		}
	}
	return result
}

// Publisher is a publisher that sends messages to a specific destination address.
type Publisher struct {
	sender         atomic.Pointer[amqp.Sender]
	connection     *AmqpConnection
	linkName       string
	destinationAdd string
	id             string
	inFlight       chan struct{}
	inFlightWg     sync.WaitGroup
	publishTimeout time.Duration
}

func (m *Publisher) Id() string {
	return m.id
}

func newPublisher(ctx context.Context, connection *AmqpConnection, destinationAdd string, options IPublisherOptions) (*Publisher, error) {
	id := fmt.Sprintf("publisher-%s", uuid.New().String())
	if options != nil && options.id() != "" {
		id = options.id()
	}

	maxInFlight := DefaultMaxInFlight
	publishTimeout := DefaultPublishTimeout
	if options != nil {
		maxInFlight = options.maxInFlight()
		publishTimeout = options.publishTimeout()
	}

	r := &Publisher{
		connection:     connection,
		linkName:       getLinkName(options),
		destinationAdd: destinationAdd,
		id:             id,
		inFlight:       make(chan struct{}, maxInFlight),
		publishTimeout: publishTimeout,
	}
	connection.entitiesTracker.storeOrReplaceProducer(r)
	err := r.createSender(ctx)
	if err != nil {
		return nil, err
	}

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

// validateMessageTarget returns an error when the publisher has no fixed
// destination and the message does not carry a valid TO property.
func (m *Publisher) validateMessageTarget(message *amqp.Message) error {
	if m.destinationAdd == "" {
		if message.Properties == nil || message.Properties.To == nil {
			return fmt.Errorf("message properties TO is required to send a message to a dynamic target address")
		}
		return validateAddress(*message.Properties.To)
	}
	return nil
}

// ensureDurable marks the message as durable when no explicit Header has been provided.
func ensureDurable(message *amqp.Message) {
	if message.Header == nil {
		message.Header = &amqp.MessageHeader{Durable: true}
	}
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
*/
func (m *Publisher) Publish(ctx context.Context, message *amqp.Message) (*PublishResult, error) {
	if err := m.validateMessageTarget(message); err != nil {
		return nil, err
	}
	ensureDurable(message)

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
		Message:              message,
		Outcome:              state,
		MessageRejectedError: extractMessageRejectedError(state),
	}, nil
}

/*
PublishAsync sends a message asynchronously and delivers the result via a callback.

The message is sent to the broker immediately (same as Publish), but the confirmation
wait (SendReceipt.Wait) happens in a background goroutine. When the broker confirms
the message or the timeout expires, the callback is invoked with the result or an error.

Back-pressure: the number of concurrent in-flight confirmations is bounded by
MaxInFlight (configurable via PublisherOptions, default 256). When the limit is
reached, PublishAsync blocks on the caller's goroutine until a slot becomes
available or the context is cancelled.

MaxInFlight could be potentially the unacked messages in the FIFO queues.

The timeout for each confirmation wait is controlled by PublisherOptions.PublishTimeout
(default 10s).

PublishAsync can increase the throughput but can increase the memory usage.
Every publish runs a goroutine that waits for the broker confirmation, so a large number of in-flight messages
can lead to a large number of goroutines and increased memory usage.
Use the MaxInFlight option to set an upper bound on the number of concurrent confirmations and control the memory usage.

callback is optional, you can set it to nil if you don't want to receive the Publish result.
*/
func (m *Publisher) PublishAsync(ctx context.Context, message *amqp.Message, callback PublishAsyncCallback) error {
	if err := m.validateMessageTarget(message); err != nil {
		return err
	}
	ensureDurable(message)

	// Acquire an in-flight slot; blocks if MaxInFlight goroutines are already waiting.
	select {
	case m.inFlight <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	receipt, err := m.sender.Load().SendWithReceipt(ctx, message, nil)
	if err != nil {
		<-m.inFlight
		return err
	}

	publishCtx := m.buildPublishContext(message)
	m.connection.metricsCollector.Publish(publishCtx)

	m.inFlightWg.Add(1)
	go func() {
		defer func() {
			<-m.inFlight
			m.inFlightWg.Done()
		}()

		waitCtx, cancel := context.WithTimeout(ctx, m.publishTimeout)
		defer cancel()

		state, waitErr := receipt.Wait(waitCtx)
		if waitErr != nil {
			if callback != nil {
				callback(nil, fmt.Errorf("publish confirmation failed: %w", waitErr))
			}
			return
		}

		m.recordPublishDisposition(state, publishCtx)
		if callback != nil {
			callback(&PublishResult{
				Message:              message,
				Outcome:              state,
				MessageRejectedError: extractMessageRejectedError(state),
			}, nil)
		}
	}()

	return nil
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
// For dynamic-target publishers (no fixed destination address), the message TO
// property is used so that OTEL semantic convention attributes are populated.
func (m *Publisher) buildPublishContext(message *amqp.Message) PublishContext {
	address := m.destinationAdd
	if address == "" && message.Properties != nil && message.Properties.To != nil {
		address = *message.Properties.To
	}
	destinationName, routingKey := parseDestinationAddress(address)

	var messageID string
	if message.Properties != nil && message.Properties.MessageID != nil {
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

// parseDestinationAddress parses an AMQP address and returns the destination name
// and routing key following OTEL semantic conventions:
// - For exchanges: "{exchange}:{routing_key}" or just "{exchange}"
// - For queues: "{queue}"
func parseDestinationAddress(address string) (destinationName, routingKey string) {
	if address == "" {
		return "", ""
	}

	// Address format: /exchanges/{exchange}/{key} or /queues/{queue}
	if strings.HasPrefix(address, "/"+exchanges+"/") {
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
		queue := strings.TrimPrefix(address, "/"+queues+"/")
		queue, _ = url.QueryUnescape(queue)
		destinationName = queue
	}

	return destinationName, routingKey
}

// Close waits for all in-flight PublishAsync confirmations to complete, then
// closes the underlying AMQP sender. No new messages should be published after
// Close is called.
func (m *Publisher) Close(ctx context.Context) error {
	m.inFlightWg.Wait()
	m.connection.entitiesTracker.removeProducer(m)
	err := m.sender.Load().Close(ctx)
	m.connection.metricsCollector.ClosePublisher()
	return err
}
