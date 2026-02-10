package rabbitmqamqp

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// Requester is an interface for making RPC (Remote Procedure Call) requests over AMQP.
// Implementations of this interface should handle the sending of requests and
// the receiving of corresponding replies, managing correlation IDs and timeouts.
//
// The default implementation provides the following behaviour:
//   - Requests are published to a specified request queue. This queue must be pre-declared.
//   - Replies are consumed from a dedicated reply-to queue. This queue is dynamically created
//     by the client.
//   - Correlation IDs are used to match requests with replies. The default implementation
//     uses a random UUID as prefix and an auto-incrementing counter as suffix.
//     The UUIDs are set as MessageID in the request message.
//   - A request timeout mechanism is in place to handle unacknowledged replies.
//   - Messages are pre-processed before publishing. The default implementation
//     assigns the correlation ID to the MessageID property of the request message.
//   - Replies are simply sent over the "callback" channel.
//
// Implementers should ensure that:
//   - `Close` properly shuts down underlying resources like publishers and consumers.
//   - `Message` provides a basic AMQP message structure for RPC requests.
//   - `Publish` sends the request message and returns a channel that will receive
//     the reply message, or be closed if a timeout occurs or the client is closed.
//   - `GetReplyQueue` returns the address of the reply queue used by the requester.
type Requester interface {
	Close(context.Context) error
	Message(body []byte) *amqp.Message
	Publish(context.Context, *amqp.Message) (<-chan *amqp.Message, error)
	GetReplyQueue() (string, error)
}

// CorrelationIdSupplier is an interface for providing correlation IDs for RPC requests.
// Implementations should generate unique identifiers for each request.
// The returned value from `Get()` should be an AMQP type, or a type that can be
// encoded into an AMQP message property (e.g., string, int, []byte, etc.).
type CorrelationIdSupplier interface {
	Get() any
}

type randomUuidCorrelationIdSupplier struct {
	mu     sync.Mutex
	prefix string
	count  int
}

func (c *randomUuidCorrelationIdSupplier) Get() any {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := fmt.Sprintf("%s-%d", c.prefix, c.count)
	c.count += 1
	return s
}

func newRandomUuidCorrelationIdSupplier() CorrelationIdSupplier {
	u, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	return &randomUuidCorrelationIdSupplier{
		prefix: u.String(),
		count:  0,
	}
}

var defaultReplyCorrelationIdExtractor CorrelationIdExtractor = func(message *amqp.Message) any {
	if message.Properties == nil || message.Properties.CorrelationID == nil {
		return nil
	}
	return message.Properties.CorrelationID
}

// RequestPostProcessor is a function that modifies an AMQP message before it is sent
// as an RPC request. It receives the message about to be sent and the correlation ID
// generated for the request. Implementations must assign the correlation ID to a
// message property (e.g., `MessageID` or `CorrelationID`) and set the `ReplyTo`
// address for the reply queue. The function must return the modified message.
//
// The default `RequestPostProcessor` implementation (used when `RequestPostProcessor`
// is not explicitly set in `RequesterOptions`) performs the following:
//   - Assigns the `correlationID` to the `MessageID` property of the `amqp.Message`.
//   - Sets the `ReplyTo` message property to a client-generated exclusive auto-delete queue.
type RequestPostProcessor func(request *amqp.Message, correlationID any) *amqp.Message

var DefaultRpcRequestTimeout = 30 * time.Second

// RequesterOptions is a struct that contains the options for the RPC client.
// It is used to configure the RPC client.
type RequesterOptions struct {
	// The name of the queue to send requests to. This queue must exist.
	//
	// Mandatory.
	RequestQueueName string
	// The name of the queue to receive replies from.
	//
	// Optional. If not set, a dedicated reply-to queue will be created for each request.
	ReplyToQueueName string
	// Generator of correlation IDs for requests. Each correlationID generated must be unique.
	//
	// Optional. If not set, a random UUID will be used as prefix and an auto-incrementing counter as suffix.
	CorrelationIdSupplier CorrelationIdSupplier
	// Function to extract correlation IDs from replies.
	//
	// Optional. If not set, the `CorrelationID` message property will be used.
	CorrelationIdExtractor CorrelationIdExtractor
	// Function to modify requests before they are sent.
	//
	// Optional. If not set, the default `RequestPostProcessor` assigns the correlation ID to the `MessageID` property.
	RequestPostProcessor RequestPostProcessor
	// The timeout for requests.
	//
	// Optional. If not set, a default timeout of 30 seconds will be used.
	RequestTimeout time.Duration

	// SettleStrategy configures how the reply consumer receives messages.
	// Use ExplicitSettle for a dedicated reply queue (default).
	// Use DirectReplyTo to enable RabbitMQ direct-reply-to (no reply queue declared).
	// See: https://www.rabbitmq.com/docs/direct-reply-to#overview
	SettleStrategy ConsumerSettleStrategy
}

type outstandingRequest struct {
	sentAt time.Time
	ch     chan *amqp.Message
	// TODO: chat to Gabriele about this: shall we communicate via an error that the request timed out?
	// or shall we just close the channel and document that if channel is closed and received is nil, it means the request timed out?
	// err    error
}

type amqpRequester struct {
	requestQueue           ITargetAddress
	replyToQueue           ITargetAddress
	publisher              *Publisher
	consumer               *Consumer
	requestPostProcessor   RequestPostProcessor
	correlationIdSupplier  CorrelationIdSupplier
	correlationIdExtractor CorrelationIdExtractor
	requestTimeout         time.Duration
	mu                     sync.Mutex
	pendingRequests        map[any]*outstandingRequest
	closed                 bool
	done                   chan struct{}
	closer                 sync.Once
}

// Close shuts down the RPC client, closing its underlying publisher and consumer.
// It ensures that all pending requests are cleaned up by closing their respective
// channels. This method is safe to call multiple times.
func (a *amqpRequester) Close(ctx context.Context) error {
	var err error
	a.closer.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.closed = true
		var err1 error
		if err1 = a.publisher.Close(ctx); err1 != nil {
			Warn("failed to close publisher", "error", err1)
		}
		var err2 error
		if err2 = a.consumer.Close(ctx); err2 != nil {
			Warn("failed to close consumer", "error", err2)
		}
		err = errors.Join(err1, err2)
		for k, req := range a.pendingRequests {
			close(req.ch)
			delete(a.pendingRequests, k)
		}
		close(a.done)
	})
	return err
}

func (a *amqpRequester) Message(body []byte) *amqp.Message {
	return amqp.NewMessage(body)
}

// Publish sends an RPC request message and returns a channel that will receive the reply.
// It first checks if the client is closed. If not, it generates a correlation ID,
// post-processes the message using the configured `RequestPostProcessor`,
// and then publishes the message. If the message is accepted by RabbitMQ,
// an `outstandingRequest` is created and stored, and a channel is returned
// for the reply. The channel will be closed if the request times out or the
// client is closed before a reply is received.
func (a *amqpRequester) Publish(ctx context.Context, message *amqp.Message) (<-chan *amqp.Message, error) {
	if a.isClosed() {
		return nil, fmt.Errorf("requester is closed")
	}
	replyTo, err := a.replyToQueue.toAddress()
	if err != nil {
		return nil, fmt.Errorf("failed to set reply-to address: %w", err)
	}
	if message.Properties == nil {
		message.Properties = &amqp.MessageProperties{}
	}
	message.Properties.ReplyTo = &replyTo
	correlationID := a.correlationIdSupplier.Get()
	m := a.requestPostProcessor(message, correlationID)
	pr, err := a.publisher.Publish(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("failed to publish request: %w", err)
	}

	switch pr.Outcome.(type) {
	case *StateAccepted:
		Debug("RabbitMQ accepted the request", "correlationID", correlationID)
	default:
		return nil, fmt.Errorf("RabbitMQ did not accept the request: %s", pr.Outcome)
	}

	ch := make(chan *amqp.Message, 1)
	a.mu.Lock()
	a.pendingRequests[correlationID] = &outstandingRequest{
		sentAt: time.Now(),
		ch:     ch,
	}
	a.mu.Unlock()
	return ch, nil
}

// GetReplyQueue returns where the Requester expects to receive replies.
// When the user sets the destination address to a dynamic address, this function will return the dynamic address.
// like direct-reply-to address. In other cases, it will return the queue address.
func (a *amqpRequester) GetReplyQueue() (string, error) {
	return a.consumer.GetQueue()
}

func (a *amqpRequester) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

// requestTimeoutTask is a goroutine that periodically checks for timed-out RPC requests.
// It runs a ticker and, when triggered, iterates through the pending requests.
// If a request's `sentAt` timestamp is older than the `requestTimeout`,
// its channel is closed, and the request is removed from `pendingRequests`.
// The goroutine exits when the `done` channel is closed, typically when the client is closed.
func (a *amqpRequester) requestTimeoutTask() {
	t := time.NewTicker(a.requestTimeout)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			limit := time.Now().Add(-a.requestTimeout)
			a.mu.Lock()
			maps.DeleteFunc(a.pendingRequests, func(k any, request *outstandingRequest) bool {
				if request.sentAt.Before(limit) {
					close(request.ch)
					Warn("request timed out", "correlationID", k)
					return true
				}
				return false
			})
			a.mu.Unlock()
		case <-a.done:
			return
		}
	}
}

// messageReceivedHandler is a goroutine that continuously receives messages from the reply queue.
// It extracts the correlation ID from each received message and attempts to match it with
// an `outstandingRequest`. If a match is found, the reply message is sent to the
// corresponding request's channel, and the request is removed from `pendingRequests`.
// If no match is found, the message is requeued. The goroutine exits when the `done`
// channel is closed, typically when the client is closed.
func (a *amqpRequester) messageReceivedHandler() {
	for {
		select {
		case <-a.done:
			Debug("rpc client message handler exited")
			return
		default:
		}

		dc, err := a.consumer.Receive(context.Background())
		if err != nil {
			Warn("failed to receive message", "error", err)
			continue
		}

		m := dc.Message()
		correlationID := a.correlationIdExtractor(m)
		a.mu.Lock()
		pendingRequest, exists := a.pendingRequests[correlationID]
		if exists {
			delete(a.pendingRequests, correlationID)
			a.mu.Unlock()
			pendingRequest.ch <- m
			close(pendingRequest.ch)
			err := dc.Accept(context.Background())
			if err != nil {
				Warn("error accepting reply", "error", err)
			}
		} else {
			a.mu.Unlock()
			Warn("received reply for unknown correlation ID", "correlationID", correlationID)
			err := dc.Requeue(context.Background())
			if err != nil {
				Warn("error requeuing reply", "error", err)
			}
		}
	}
}
