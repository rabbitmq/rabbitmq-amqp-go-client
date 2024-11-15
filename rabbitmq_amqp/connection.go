package rabbitmq_amqp

import (
	"context"
	"crypto/tls"
	"fmt"
)

type TSaslMechanism string

const (
	Plain     TSaslMechanism = "plain"
	External  TSaslMechanism = "external"
	Anonymous TSaslMechanism = "anonymous"
)

type SaslMechanism struct {
	Type TSaslMechanism
}

type ConnectionSettings struct {
	Host          string
	Port          int
	User          string
	Password      string
	VirtualHost   string
	Scheme        string
	ContainerId   string
	UseSsl        bool
	TlsConfig     *tls.Config
	SaslMechanism TSaslMechanism
}

func (c *ConnectionSettings) BuildAddress() string {
	return c.Scheme + "://" + c.Host + ":" + fmt.Sprint(c.Port)
}

// NewConnectionSettings creates a new ConnectionSettings struct with default values.
func NewConnectionSettings() *ConnectionSettings {
	return &ConnectionSettings{
		Host:        "localhost",
		Port:        5672,
		User:        "guest",
		Password:    "guest",
		VirtualHost: "/",
		Scheme:      "amqp",
		ContainerId: "amqp-go-client",
		UseSsl:      false,
		TlsConfig:   nil,
	}
}

type IConnection interface {
	// Open opens a connection to the AMQP 1.0 server.
	Open(ctx context.Context, connectionSettings *ConnectionSettings) error

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
}
