package pkg

import (
	"context"
	"crypto/tls"
)

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
	GetSsl() bool
	BuildAddress() string
	TlsConfig(config *tls.Config) IConnectionSettings
	GetTlsConfig() *tls.Config
}

type IConnection interface {
	Open(ctx context.Context, connectionSettings IConnectionSettings) error
	Close(ctx context.Context) error
	Management() IManagement
}
