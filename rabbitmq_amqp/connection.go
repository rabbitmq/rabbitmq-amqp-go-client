package rabbitmq_amqp

import (
	"context"
	"crypto/tls"
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

type IConnectionSettings interface {
	GetHost() string
	Host(hostName string) IConnectionSettings
	GetPort() int
	Port(port int) IConnectionSettings
	GetUser() string
	User(userName string) IConnectionSettings
	GetPassword() string
	Password(password string) IConnectionSettings
	GetVirtualHost() string
	VirtualHost(virtualHost string) IConnectionSettings
	GetScheme() string
	GetContainerId() string
	ContainerId(containerId string) IConnectionSettings
	UseSsl(value bool) IConnectionSettings
	IsSsl() bool
	BuildAddress() string
	TlsConfig(config *tls.Config) IConnectionSettings
	GetTlsConfig() *tls.Config
	GetSaslMechanism() TSaslMechanism
	SaslMechanism(mechanism SaslMechanism) IConnectionSettings
}

type IConnection interface {
	Open(ctx context.Context, connectionSettings IConnectionSettings) error
	Close(ctx context.Context) error
	Management() IManagement
	NotifyStatusChange(channel chan *StatusChanged)
	GetStatus() int
}
