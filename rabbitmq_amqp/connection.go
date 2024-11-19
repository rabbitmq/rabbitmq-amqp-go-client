package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type IConnection interface {

	// Close closes the connection to the AMQP 1.0 server.
	Close(ctx context.Context) error

	// Management returns the management interface for the connection.
	Management() IManagement

	// NotifyStatusChange registers a channel to receive status change notifications.
	// The channel will receive a StatusChanged struct whenever the status of the connection changes.
	NotifyStatusChange(channel chan *StatusChanged)
	// Status returns the current status of the connection.
	// See LifeCycle struct for more information.
	Status() int

	// Publisher returns a new IPublisher interface for the connection.
	Publisher(ctx context.Context, destinationAddr string, linkName string) (IPublisher, error)
}

// IPublisher is an interface for publishers messages based.
// on the AMQP 1.0 protocol.
type IPublisher interface {
	Publish(ctx context.Context, message *amqp.Message) error
	Close(ctx context.Context) error
}
