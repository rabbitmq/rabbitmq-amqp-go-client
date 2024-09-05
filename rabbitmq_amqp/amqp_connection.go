package rabbitmq_amqp

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Azure/go-amqp"
)

type ConnectionSettings struct {
	host        string
	port        int
	user        string
	password    string
	virtualHost string
	scheme      string
	containerId string
	useSsl      bool
	tlsConfig   *tls.Config
}

func (c *ConnectionSettings) TlsConfig(config *tls.Config) IConnectionSettings {
	c.tlsConfig = config
	return c
}

func (c *ConnectionSettings) GetTlsConfig() *tls.Config {
	return c.tlsConfig
}

func (c *ConnectionSettings) Port(port int) IConnectionSettings {
	c.port = port
	return c
}

func (c *ConnectionSettings) User(userName string) IConnectionSettings {

	c.user = userName
	return c
}

func (c *ConnectionSettings) Password(password string) IConnectionSettings {
	c.password = password
	return c
}

func (c *ConnectionSettings) VirtualHost(virtualHost string) IConnectionSettings {
	c.virtualHost = virtualHost
	return c
}

func (c *ConnectionSettings) ContainerId(containerId string) IConnectionSettings {
	c.containerId = containerId
	return c
}

func (c *ConnectionSettings) GetHost() string {
	return c.host
}

func (c *ConnectionSettings) Host(hostName string) IConnectionSettings {
	c.host = hostName
	return c

}

func (c *ConnectionSettings) GetPort() int {
	return c.port
}

func (c *ConnectionSettings) GetUser() string {
	return c.user
}

func (c *ConnectionSettings) GetPassword() string {
	return c.password
}

func (c *ConnectionSettings) GetVirtualHost() string {
	return c.virtualHost
}

func (c *ConnectionSettings) GetScheme() string {
	return c.scheme
}

func (c *ConnectionSettings) GetContainerId() string {
	return c.containerId
}

func (c *ConnectionSettings) UseSsl(value bool) IConnectionSettings {
	c.useSsl = value
	if value {
		c.scheme = "amqps"
	} else {
		c.scheme = "amqp"
	}
	return c
}

func (c *ConnectionSettings) GetSsl() bool {
	return c.useSsl
}

func (c *ConnectionSettings) BuildAddress() string {
	return c.scheme + "://" + c.host + ":" + fmt.Sprint(c.port)
}

func NewConnectionSettings() IConnectionSettings {
	return &ConnectionSettings{
		host:        "localhost",
		port:        5672,
		user:        "guest",
		password:    "guest",
		virtualHost: "/",
		scheme:      "amqp",
		containerId: "amqp-go-client",
		useSsl:      false,
		tlsConfig:   nil,
	}
}

type AmqpConnection struct {
	Connection *amqp.Conn
	management IManagement
}

func (a *AmqpConnection) Management() IManagement {
	return a.management
}

func NewAmqpConnection() IConnection {
	return &AmqpConnection{
		management: NewAmqpManagement(),
	}
}

func (a *AmqpConnection) Open(ctx context.Context, connectionSettings IConnectionSettings) error {
	sASLType := amqp.SASLTypePlain(connectionSettings.GetUser(), connectionSettings.GetPassword())

	if connectionSettings.GetSsl() {
		sASLType = amqp.SASLTypeExternal("")
	}

	conn, err := amqp.Dial(ctx, connectionSettings.BuildAddress(), &amqp.ConnOptions{
		ContainerID: connectionSettings.GetContainerId(),
		SASLType:    sASLType,
		HostName:    connectionSettings.GetVirtualHost(),
		TLSConfig:   connectionSettings.GetTlsConfig(),
	})
	if err != nil {
		return err
	}
	a.Connection = conn
	err = a.Management().Open(ctx, a)
	if err != nil {
		return err
	}
	return nil
}

func (a *AmqpConnection) Close(ctx context.Context) error {
	err := a.Management().Close(ctx)
	if err != nil {
		return err
	}
	return a.Connection.Close()
}
