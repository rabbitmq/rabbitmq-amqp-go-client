package rabbitmq_amqp

import (
	"context"
)

type IManagement interface {
	// Open setups the sender and receiver links to the management interface.
	Open(ctx context.Context, connection IConnection) error
	// Close closes the sender and receiver links to the management interface.
	Close(ctx context.Context) error

	// DeclareQueue creates a queue with the specified specification.
	DeclareQueue(ctx context.Context, specification *QueueSpecification) (IQueueInfo, error)
	// DeleteQueue deletes the queue with the specified name.
	DeleteQueue(ctx context.Context, name string) error
	// DeclareExchange creates an exchange with the specified specification.
	DeclareExchange(ctx context.Context, exchangeSpecification *ExchangeSpecification) (IExchangeInfo, error)
	// DeleteExchange deletes the exchange with the specified name.
	DeleteExchange(ctx context.Context, name string) error
	//Bind creates a binding between an exchange and a queue or exchange
	Bind(ctx context.Context, bindingSpecification *BindingSpecification) (string, error)
	// Unbind removes a binding between an exchange and a queue or exchange given the binding path.
	Unbind(ctx context.Context, bindingPath string) error
	// PurgeQueue removes all messages from the queue. Returns the number of messages purged.
	PurgeQueue(ctx context.Context, queueName string) (int, error)
	// QueueInfo returns information about the queue with the specified name.
	QueueInfo(ctx context.Context, queueName string) (IQueueInfo, error)

	// Status returns the current status of the management interface.
	// See LifeCycle struct for more information.
	Status() int

	// NotifyStatusChange registers a channel to receive status change notifications.
	// The channel will receive a StatusChanged struct whenever the status of the management interface changes.
	NotifyStatusChange(channel chan *StatusChanged)

	//Request sends a request to the management interface with the specified body, path, and method.
	//Returns the response body as a map[string]any.
	//It usually is not necessary to call this method directly. Leave it public for custom use cases.
	// The calls above are the recommended way to interact with the management interface.
	Request(ctx context.Context, body any, path string, method string,
		expectedResponseCodes []int) (map[string]any, error)
}
