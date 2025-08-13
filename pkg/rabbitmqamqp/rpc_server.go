package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

type RpcServerHandler func(ctx context.Context, request *amqp.Message) (*amqp.Message, error)

var noOpHandler RpcServerHandler = func(_ context.Context, _ *amqp.Message) (*amqp.Message, error) {
	return nil, nil
}

// CorrelationIdExtractor defines the signature for a function that extracts the correlation ID
// from an AMQP message. Then returned value must be a valid AMQP type that can be binary encoded.
type CorrelationIdExtractor func(message *amqp.Message) any

var defaultCorrelationIdExtractor CorrelationIdExtractor = func(message *amqp.Message) any {
	if message.Properties == nil {
		return nil
	}
	return message.Properties.MessageID
}

// ReplyPostProcessor is a function that is called after the request handler has processed the request.
// It can be used to modify the reply message before it is sent.
type ReplyPostProcessor func(reply *amqp.Message, correlationID any) *amqp.Message

var defaultReplyPostProcessor ReplyPostProcessor = func(reply *amqp.Message, correlationID any) *amqp.Message {
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
	// Unpause requests to receive messages again.
	Unpause() error
}

type RpcServerOptions struct {
	// RequestQueue is the name of the queue to subscribe to. This queue must be pre-declared.
	// The RPC server does not declare the queue, it is the responsibility of the caller to declare the queue.
	RequestQueue string
	// Handler is a function to process the request message. If the server wants to send a response to
	// the client, it must return a response message. If the function returns nil, the server will not send a response.
	//
	// It is encouraged to initialise the response message properties in the handler. If the handler returns a non-nil
	// error, the server will discard the request message and log an error.
	//
	// The server handler blocks until this function returns. It is highly recommended to functions that process and return quickly.
	// If you need to perform a long running operation, it's advisable to dispatch the operation to another queue.
	//
	// Example:
	// 		func(ctx context.Context, request *amqp.Message) (*amqp.Message, error) {
	// 			return amqp.NewMessage([]byte(fmt.Sprintf("Pong: %s", request.GetData()))), nil
	// 		}
	Handler RpcServerHandler
	// CorrectionIdExtractor is a function that extracts a correction ID from the request message.
	// The returned value should be an AMQP type that can be binary encoded.
	//
	// This field is optional. If not provided, the server will use the MessageID as the correlation ID.
	//
	// Example:
	// 		func(message *amqp.Message) any {
	// 			return message.Properties.MessageID
	// 		}
	//
	// The default correlation ID extractor also handles nil cases.
	CorrelationIdExtractor CorrelationIdExtractor
	// PostProcessor is a function that receives the reply message and the extracted correlation ID, just before the reply is sent.
	// It can be used to modify the reply message before it is sent.
	//
	// The post processor must set the correlation ID in the reply message properties.
	//
	// This field is optional. If not provided, the server will set the correlation ID in the reply message properties, using
	// the correlation ID extracted from the CorrelationIdExtractor.
	//
	// Example:
	// 		func(reply *amqp.Message, correlationID any) *amqp.Message {
	// 			reply.Properties.CorrelationID = correlationID
	// 			return reply
	// 		}
	//
	// The default post processor also handles nil cases.
	ReplyPostProcessor ReplyPostProcessor
}

func (r *RpcServerOptions) validate() error {
	if r.RequestQueue == "" {
		return fmt.Errorf("requestQueue is mandatory")
	}
	return nil
}

type amqpRpcServer struct {
	// TODO: handle state changes for reconnections
	mu                     sync.Mutex
	requestHandler         RpcServerHandler
	requestQueue           string
	publisher              *Publisher
	consumer               *Consumer
	closer                 sync.Once
	closed                 bool
	correlationIdExtractor CorrelationIdExtractor
	replyPostProcessor     ReplyPostProcessor
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
	err := a.consumer.pause(context.Background())
	if err != nil {
		Warn("Did not pause consumer", "error", err)
	}
}

func (a *amqpRpcServer) Unpause() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()

	err := a.consumer.unpause(1)
	if err != nil {
		return fmt.Errorf("error unpausing RPC server: %w", err)
	}
	return nil
}

func (a *amqpRpcServer) handle() {
	/*
		The RPC server has the following behavior:
		- when receiving a message request:
			- it calls the processing logic (handler)
			- it extracts the correlation ID
			- it calls a reply post-processor if defined
			- it sends the reply message
		- if all these operations succeed, the server accepts the request message (settles it with the ACCEPTED outcome)
		- if any of these operations throws an exception, the server discards the request message (the message is
			removed from the request queue and is dead-lettered if configured)
	*/
	for {
		if a.isClosed() {
			Debug("RPC server is closed. Stopping the handler")
			return
		}

		err := a.issueCredits(1)
		if err != nil {
			Warn("Failed to request credits", "error", err)
			continue
		}

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
		reply = a.replyPostProcessor(reply, correlationID)
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

func (a *amqpRpcServer) issueCredits(credits uint32) error {
	return a.consumer.issueCredits(credits)
}
