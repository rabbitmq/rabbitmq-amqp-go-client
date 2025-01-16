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
	management *AmqpManagement
	lifeCycle  *LifeCycle
	session    *amqp.Session
}

func (a *AmqpConnection) Publisher(ctx context.Context, destinationAdd string, linkName string) (*Publisher, error) {
	if !validateAddress(destinationAdd) {
		return nil, fmt.Errorf("invalid destination address, the address should start with /%s/ or/%s/ ", exchanges, queues)
	}

	sender, err := a.session.NewSender(ctx, destinationAdd, createSenderLinkOptions(destinationAdd, linkName, AtLeastOnce))
	if err != nil {
		return nil, err
	}
	return newPublisher(sender), nil
}

// Dial connect to the AMQP 1.0 server using the provided connectionSettings
// Returns a pointer to the new AmqpConnection if successful else an error.
// addresses is a list of addresses to connect to. It picks one randomly.
// It is enough that one of the addresses is reachable.
func Dial(ctx context.Context, addresses []string, connOptions *amqp.ConnOptions) (*AmqpConnection, error) {
	conn := &AmqpConnection{
		management: NewAmqpManagement(),
		lifeCycle:  NewLifeCycle(),
	}
	tmp := make([]string, len(addresses))
	copy(tmp, addresses)

	// random pick and extract one address to use for connection
	for len(tmp) > 0 {
		idx := random(len(tmp))
		addr := tmp[idx]
		// remove the index from the tmp list
		tmp = append(tmp[:idx], tmp[idx+1:]...)
		err := conn.open(ctx, addr, connOptions)
		if err != nil {
			Error("Failed to open connection", ExtractWithoutPassword(addr), err)
			continue
		}
		Debug("Connected to", ExtractWithoutPassword(addr))
		return conn, nil
	}
	return nil, fmt.Errorf("no address to connect to")
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
	err = a.management.Open(ctx, a)
	if err != nil {
		// TODO close connection?
		return err
	}

	a.lifeCycle.SetState(&StateOpen{})
	return nil
}

func (a *AmqpConnection) Close(ctx context.Context) error {
	err := a.management.Close(ctx)
	if err != nil {
		return err
	}
	err = a.Connection.Close()
	a.lifeCycle.SetState(&StateClosed{})
	return err
}

// NotifyStatusChange registers a channel to receive getState change notifications
// from the connection.
func (a *AmqpConnection) NotifyStatusChange(channel chan *StateChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpConnection) State() LifeCycleState {
	return a.lifeCycle.State()
}

// *** management section ***

// Management returns the management interface for the connection.
// The management interface is used to declare and delete exchanges, queues, and bindings.
func (a *AmqpConnection) Management() *AmqpManagement {
	return a.management
}

//*** end management section ***
