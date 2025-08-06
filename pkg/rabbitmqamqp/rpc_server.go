package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// TODO: move name supplier to Rpc Client
type nameSupplier struct {
	prefix string
}

func newNameSupplier(prefix string) *nameSupplier {
	return &nameSupplier{prefix: prefix}
}

func (n *nameSupplier) get() string {
	return fmt.Sprintf("%s-%s", n.prefix, uuid.New().String())
}

type RpcServerHandler func(ctx context.Context, request *amqp.Message) (*amqp.Message, error)

var noOpHandler RpcServerHandler = func(_ context.Context, _ *amqp.Message) (*amqp.Message, error) {
	return nil, nil
}

// CorrelationIdExtractor defines the signature for a function that extracts the correlation ID
// from an AMQP message.
type CorrelationIdExtractor func(message *amqp.Message) any

var defaultCorrelationIdExtractor CorrelationIdExtractor = func(message *amqp.Message) any {
	if message.Properties == nil {
		return nil
	}
	return message.Properties.MessageID
}

type PostProcessor func(reply *amqp.Message, correlationID any) *amqp.Message

var defaultPostProcessor PostProcessor = func(reply *amqp.Message, correlationID any) *amqp.Message {
	if reply == nil {
		return nil
	}
	if reply.Properties == nil {
		reply.Properties = &amqp.MessageProperties{}
	}
	reply.Properties.CorrelationID = correlationID
	return reply
}

// RpcServer is Remote Procedure Call server that receives a message, process them,
// and sends a response.
type RpcServer interface {
	// Close the RPC server and its underlying resources.
	Close(context.Context) error
	// Pause the server to stop receiving messages.
	Pause()
	// Unpause request to receive messages again.
	Unpause()
	// Handle receives an RPC request message, process it, and returns a response message.
	// Handle(context.Context, *amqp.Message) *amqp.Message
}

type RpcServerOptions struct {
	RequestQueue string
	Handler      RpcServerHandler
	// CorrectionIdExtractor is a function that extracts a correction ID from the request message.
	// The returned value should be an AMQP type that can be binary encoded.
	CorrelationIdExtractor CorrelationIdExtractor
	PostProcessor          PostProcessor
}

type amqpRpcServer struct {
	mu                     sync.Mutex
	requestHandler         RpcServerHandler
	requestQueue           string
	publisher              *Publisher
	consumer               *Consumer
	closer                 sync.Once
	closed                 bool
	correlationIdExtractor CorrelationIdExtractor
	postProcessor          PostProcessor
}

// Close closes the RPC server and its underlying AMQP resources. It ensures that these resources
// are closed gracefully and only once, even if Close is called multiple times.
// The provided context (ctx) controls the timeout for the close operation, ensuring the operation
// does not exceed the context's deadline.
func (a *amqpRpcServer) Close(ctx context.Context) error {
	// TODO: wait for unsettled messages
	a.closer.Do(func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.closed = true
		// TODO: set a context timeout for the publisher and consumer close operations
		if a.publisher != nil {
			err := a.publisher.Close(ctx)
			if err != nil {
				Error("Failed to close publisher", "error", err)
			}
		}
		if a.consumer != nil {
			err := a.consumer.Close(ctx)
			if err != nil {
				Error("Failed to close consumer", "error", err)
			}
		}
	})
	return nil
}

func (a *amqpRpcServer) Pause() {
	//TODO implement me
	panic("implement me")
}

func (a *amqpRpcServer) Unpause() {
	//TODO implement me
	panic("implement me")
}

func (a *amqpRpcServer) handle() {
	// This function handles all the server behaviour, tweaking points are correectionIdExtractor function and
	// post processor function.
	// TODO: implement these
	/*
		The RPC server has the following behavior:
		when receiving a message request, it calls the processing logic (handler), extracts the correlation ID,
		calls a reply post-processor if defined, and sends the reply message.
		if all these operations succeed, the server accepts the request message (settles it with the ACCEPTED outcome).
		if any of these operations throws an exception, the server discards the request message (the message is
		removed from the request queue and is dead-lettered if configured).
	*/
	for {
		if a.isClosed() {
			Debug("RPC server is closed. Stopping the handler")
			return
		}
		// TODO: maybe we need to panic to unblock the call
		request, err := a.consumer.Receive(context.Background())
		if err != nil {
			Debug("Receive request returned error. This may be expected if the server is closing", "error", err)
			continue
		}
		// TODO: add a configurable timeout for the request handling
		reply, err := a.requestHandler(context.Background(), request.message)
		if err != nil {
			Error("Request handler returned error. Discarding request", "error", err)
			request.Discard(context.Background(), nil)
			continue
		}

		if reply != nil && request.message.Properties != nil && request.message.Properties.ReplyTo != nil {
			setToProperty(reply, request.message.Properties.ReplyTo)
		}

		correlationID := a.correlationIdExtractor(request.message)
		reply = a.postProcessor(reply, correlationID)
		if reply != nil {
			err = callAndMaybeRetry(func() error {
				r, err := a.publisher.Publish(context.Background(), reply)
				if err != nil {
					return err
				}
				switch r.Outcome.(type) {
				case *StateAccepted:
					return nil
				}
				return fmt.Errorf("reply message not accepted: %s", r.Outcome)
			}, []time.Duration{time.Second, 3 * time.Second, 5 * time.Second, 10 * time.Second})
			if err != nil {
				Error("Failed to publish reply", "error", err, "correlationId", reply.Properties.CorrelationID)
				request.Discard(context.Background(), nil)
				continue
			}
		}

		// TODO: https://github.com/rabbitmq/rabbitmq-amqp-java-client/blob/main/src/main/java/com/rabbitmq/client/amqp/impl/AmqpRpcServer.java#L100-L103
		err = request.Accept(context.Background())
		if err != nil {
			Error("Failed to accept request", "error", err, "messageId", request.message.Properties.MessageID)
		}
	}
}

func (a *amqpRpcServer) isClosed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

func callAndMaybeRetry(fn func() error, delays []time.Duration) error {
	var err error
	for i, delay := range delays {
		err = fn()
		if err == nil {
			return nil
		}
		Error("Retrying operation", "attempt", i+1, "error", err)
		if i < len(delays)-1 { // Don't sleep after the last attempt
			time.Sleep(delay)
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", len(delays), err)
}

// setToProperty sets the To property of the message m to the value of replyTo.
// If the message has no properties, it creates a new properties object.
// This function modifies the message in place.
func setToProperty(m *amqp.Message, replyTo *string) {
	if m.Properties == nil {
		m.Properties = &amqp.MessageProperties{}
	}
	m.Properties.To = replyTo
}
