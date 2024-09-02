package pkg

import (
	"context"
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

func (c *ConnectionSettings) Scheme(scheme string) IConnectionSettings {
	c.scheme = scheme
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
	}
}

type AmqpConnection struct {
	Connection *amqp.Conn
	Session    *amqp.Session
}

func NewAmqpConnection() IConnection {
	return &AmqpConnection{}
}

func (a *AmqpConnection) Open(ctx context.Context, connectionSettings IConnectionSettings) error {
	conn, err := amqp.Dial(ctx, connectionSettings.BuildAddress(), &amqp.ConnOptions{
		ContainerID: connectionSettings.GetContainerId(),
		SASLType:    amqp.SASLTypePlain(connectionSettings.GetUser(), connectionSettings.GetPassword()),
		HostName:    connectionSettings.GetVirtualHost(),
	})
	if err != nil {
		return err
	}
	a.Connection = conn
	return nil
}

func (a *AmqpConnection) Close() error {
	return a.Connection.Close()
}
