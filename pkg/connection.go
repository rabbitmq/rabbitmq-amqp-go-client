package pkg

import "context"

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
	Scheme(scheme string) IConnectionSettings
	GetContainerId() string
	ContainerId(containerId string) IConnectionSettings
	BuildAddress() string
}

type IConnection interface {
	Open(ctx context.Context, connectionSettings IConnectionSettings) error
	Close() error
}
