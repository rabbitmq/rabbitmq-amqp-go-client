package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

//func (c *ConnectionSettings) UseSsl(value bool) {
//	c.UseSsl = value
//	if value {
//		c.Scheme = "amqps"
//	} else {
//		c.Scheme = "amqp"
//	}
//}

type AmqpConnection struct {
	Connection *amqp.Conn
	management IManagement
	lifeCycle  *LifeCycle
}

func (a *AmqpConnection) Management() IManagement {
	return a.management
}

// NewAmqpConnection creates a new AmqpConnection
// with a new AmqpManagement and a new LifeCycle.
// Returns a pointer to the new AmqpConnection
func NewAmqpConnection() IConnection {
	return &AmqpConnection{
		management: NewAmqpManagement(),
		lifeCycle:  NewLifeCycle(),
	}
}

// NewAmqpConnectionNotifyStatusChanged creates a new AmqpConnection
// with a new AmqpManagement and a new LifeCycle
// and sets the channel for status changes.
// Returns a pointer to the new AmqpConnection
func NewAmqpConnectionNotifyStatusChanged(channel chan *StatusChanged) IConnection {
	lifeCycle := NewLifeCycle()
	lifeCycle.chStatusChanged = channel
	return &AmqpConnection{
		management: NewAmqpManagement(),
		lifeCycle:  lifeCycle,
	}
}

func (a *AmqpConnection) Open(ctx context.Context, connectionSettings *ConnectionSettings) error {
	sASLType := amqp.SASLTypeAnonymous()
	switch connectionSettings.SaslMechanism {
	case Plain:
		sASLType = amqp.SASLTypePlain(connectionSettings.User, connectionSettings.Password)
	case External:
		sASLType = amqp.SASLTypeExternal("")
	}

	conn, err := amqp.Dial(ctx, connectionSettings.BuildAddress(), &amqp.ConnOptions{
		ContainerID: connectionSettings.ContainerId,
		SASLType:    sASLType,
		HostName:    connectionSettings.VirtualHost,
		TLSConfig:   connectionSettings.TlsConfig,
	})
	if err != nil {
		return err
	}
	a.Connection = conn
	a.lifeCycle.SetStatus(Open)

	err = a.Management().Open(ctx, a)
	if err != nil {
		// TODO close connection?
		return err
	}
	return nil
}

func (a *AmqpConnection) Close(ctx context.Context) error {
	err := a.Management().Close(ctx)
	if err != nil {
		return err
	}
	err = a.Connection.Close()
	a.lifeCycle.SetStatus(Closed)
	return err
}

func (a *AmqpConnection) NotifyStatusChange(channel chan *StatusChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpConnection) Status() int {
	return a.lifeCycle.Status()
}
