package rabbitmq_amqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
)

//func (c *ConnUrlHelper) UseSsl(value bool) {
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
	session    *amqp.Session
}

func (a *AmqpConnection) Publisher(ctx context.Context, destinationAdd string, linkName string) (IPublisher, error) {
	sender, err := a.session.NewSender(ctx, destinationAdd, createSenderLinkOptions(destinationAdd, linkName))
	if err != nil {
		return nil, err
	}
	return newPublisher(sender), nil
}

// Management returns the management interface for the connection.
// See IManagement interface.
func (a *AmqpConnection) Management() IManagement {
	return a.management
}

// Dial creates a new AmqpConnection
// with a new AmqpManagement and a new LifeCycle.
// Returns a pointer to the new AmqpConnection
func Dial(ctx context.Context, addr string, connOptions *amqp.ConnOptions) (IConnection, error) {
	conn := &AmqpConnection{
		management: NewAmqpManagement(),
		lifeCycle:  NewLifeCycle(),
	}
	err := conn.open(ctx, addr, connOptions)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Open opens a connection to the AMQP 1.0 server.
// using the provided connectionSettings and the AMQPLite library.
// Setups the connection and the management interface.
func (a *AmqpConnection) open(ctx context.Context, addr string, connOptions *amqp.ConnOptions) error {

	if connOptions == nil {
		connOptions = &amqp.ConnOptions{
			// RabbitMQ requires SASL security layer
			// to be enabled for AMQP 1.0 connections.
			// So this is mandatory and default in case not defined.
			SASLType: amqp.SASLTypeAnonymous(),
		}
	}

	//connOptions.HostName is the  way to set the virtual host
	// so we need to pre-parse the URI to get the virtual host
	// the PARSE is copied from go-amqp091 library
	// the URI will be parsed is parsed again in the amqp lite library
	uri, err := ParseURI(addr)
	if err != nil {
		return err
	}
	connOptions.HostName = fmt.Sprintf("vhost:%s", uri.Vhost)

	conn, err := amqp.Dial(ctx, addr, connOptions)
	if err != nil {
		return err
	}
	a.Connection = conn
	a.session, err = a.Connection.NewSession(ctx, nil)
	if err != nil {
		return err
	}
	err = a.Management().Open(ctx, a)
	if err != nil {
		// TODO close connection?
		return err
	}

	a.lifeCycle.SetStatus(Open)
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
