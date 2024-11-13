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
	Open(ctx context.Context, connectionSettings *ConnectionSettings) error
	Close(ctx context.Context) error
	Management() IManagement
	NotifyStatusChange(channel chan *StatusChanged)
	Status() int
}
