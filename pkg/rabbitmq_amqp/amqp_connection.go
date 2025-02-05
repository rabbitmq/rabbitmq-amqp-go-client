package rabbitmq_amqp

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"sync"
	"time"
)

//func (c *ConnUrlHelper) UseSsl(value bool) {
//	c.UseSsl = value
//	if value {
//		c.Scheme = "amqps"
//	} else {
//		c.Scheme = "amqp"
//	}
//}

type AmqpConnOptions struct {
	// wrapper for amqp.ConnOptions
	ContainerID string
	// wrapper for amqp.ConnOptions
	HostName string
	// wrapper for amqp.ConnOptions
	IdleTimeout time.Duration

	// wrapper for amqp.ConnOptions
	MaxFrameSize uint32

	// wrapper for amqp.ConnOptions
	MaxSessions uint16

	// wrapper for amqp.ConnOptions
	Properties map[string]any

	// wrapper for amqp.ConnOptions
	SASLType amqp.SASLType

	// wrapper for amqp.ConnOptions
	TLSConfig *tls.Config

	// wrapper for amqp.ConnOptions
	WriteTimeout time.Duration

	// RecoveryConfiguration is used to configure the recovery behavior of the connection.
	// when the connection is closed unexpectedly.
	RecoveryConfiguration *RecoveryConfiguration

	// copy the addresses for reconnection
	addresses []string
}

type AmqpConnection struct {
	Connection      *amqp.Conn
	id              string
	management      *AmqpManagement
	lifeCycle       *LifeCycle
	amqpConnOptions *AmqpConnOptions
	session         *amqp.Session
	refMap          *sync.Map
	entitiesTracker *entitiesTracker
}

// NewPublisher creates a new Publisher that sends messages to the provided destination.
// The destination is a TargetAddress that can be a Queue or an Exchange with a routing key.
// See QueueAddress and ExchangeAddress for more information.
func (a *AmqpConnection) NewPublisher(ctx context.Context, destination TargetAddress, linkName string) (*Publisher, error) {
	destinationAdd := ""
	err := error(nil)
	if destination != nil {
		destinationAdd, err = destination.toAddress()
		if err != nil {
			return nil, err
		}
		err = validateAddress(destinationAdd)
		if err != nil {
			return nil, err
		}
	}

	return newPublisher(ctx, a, destinationAdd, linkName)
}

// NewConsumer creates a new Consumer that listens to the provided destination. Destination is a QueueAddress.
func (a *AmqpConnection) NewConsumer(ctx context.Context, destination *QueueAddress, linkName string) (*Consumer, error) {
	destinationAdd, err := destination.toAddress()
	if err != nil {
		return nil, err
	}
	err = validateAddress(destinationAdd)

	if err != nil {
		return nil, err
	}
	receiver, err := a.session.NewReceiver(ctx, destinationAdd, createReceiverLinkOptions(destinationAdd, linkName, AtLeastOnce))
	if err != nil {
		return nil, err
	}
	return newConsumer(receiver), nil
}

// Dial connect to the AMQP 1.0 server using the provided connectionSettings
// Returns a pointer to the new AmqpConnection if successful else an error.
// addresses is a list of addresses to connect to. It picks one randomly.
// It is enough that one of the addresses is reachable.
func Dial(ctx context.Context, addresses []string, connOptions *AmqpConnOptions, args ...string) (*AmqpConnection, error) {
	if connOptions == nil {
		connOptions = &AmqpConnOptions{
			// RabbitMQ requires SASL security layer
			// to be enabled for AMQP 1.0 connections.
			// So this is mandatory and default in case not defined.
			SASLType: amqp.SASLTypeAnonymous(),
		}
	}

	if connOptions.RecoveryConfiguration == nil {
		connOptions.RecoveryConfiguration = NewRecoveryConfiguration()
	}

	conn := &AmqpConnection{
		management:      NewAmqpManagement(),
		lifeCycle:       NewLifeCycle(),
		amqpConnOptions: connOptions,
		entitiesTracker: newEntitiesTracker(),
	}
	tmp := make([]string, len(addresses))
	copy(tmp, addresses)

	err := conn.open(ctx, addresses, connOptions, args...)
	if err != nil {
		return nil, err
	}
	conn.amqpConnOptions = connOptions
	conn.amqpConnOptions.addresses = addresses
	return conn, nil

}

// Open opens a connection to the AMQP 1.0 server.
// using the provided connectionSettings and the AMQPLite library.
// Setups the connection and the management interface.
func (a *AmqpConnection) open(ctx context.Context, addresses []string, connOptions *AmqpConnOptions, args ...string) error {

	amqpLiteConnOptions := &amqp.ConnOptions{
		ContainerID:  connOptions.ContainerID,
		HostName:     connOptions.HostName,
		IdleTimeout:  connOptions.IdleTimeout,
		MaxFrameSize: connOptions.MaxFrameSize,
		MaxSessions:  connOptions.MaxSessions,
		Properties:   connOptions.Properties,
		SASLType:     connOptions.SASLType,
		TLSConfig:    connOptions.TLSConfig,
		WriteTimeout: connOptions.WriteTimeout,
	}
	tmp := make([]string, len(addresses))
	copy(tmp, addresses)

	// random pick and extract one address to use for connection
	var conn *amqp.Conn
	for len(tmp) > 0 {
		idx := random(len(tmp))
		addr := tmp[idx]
		//connOptions.HostName is the  way to set the virtual host
		// so we need to pre-parse the URI to get the virtual host
		// the PARSE is copied from go-amqp091 library
		// the URI will be parsed is parsed again in the amqp lite library
		uri, err := ParseURI(addr)
		if err != nil {
			return err
		}
		connOptions.HostName = fmt.Sprintf("vhost:%s", uri.Vhost)
		// remove the index from the tmp list
		tmp = append(tmp[:idx], tmp[idx+1:]...)
		conn, err = amqp.Dial(ctx, addr, amqpLiteConnOptions)
		if err != nil {
			Error("Failed to open connection", ExtractWithoutPassword(addr), err)
			continue
		}
		Debug("Connected to", ExtractWithoutPassword(addr))
		break
	}
	if conn == nil {
		return fmt.Errorf("failed to connect to any of the provided addresses")
	}

	if len(args) > 0 {
		a.id = args[0]
	} else {
		a.id = uuid.New().String()
	}

	a.Connection = conn
	var err error
	a.session, err = a.Connection.NewSession(ctx, nil)
	go func() {
		select {
		case <-conn.Done():
			{
				if conn.Err() != nil {
					Error("Connection closed unexpectedly", "error", conn.Err())
					a.maybeReconnect()
					return
				}

				Debug("Connection closed successfully")
			}
		}
	}()

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
func (a *AmqpConnection) maybeReconnect() {
	a.lifeCycle.SetState(&StateReconnecting{})

	if !a.amqpConnOptions.RecoveryConfiguration.ActiveRecovery {
		Info("Recovery is disabled, closing connection")
		return
	}
	numberOfAttempts := 1
	waitTime := a.amqpConnOptions.RecoveryConfiguration.BackOffReconnectInterval
	for numberOfAttempts <= a.amqpConnOptions.RecoveryConfiguration.MaxReconnectAttempts {
		///wait for before reconnecting
		Info("Waiting before reconnecting", "in", waitTime, "attempt", numberOfAttempts)
		time.Sleep(waitTime)
		// context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		// try to createSender
		err := a.open(ctx, a.amqpConnOptions.addresses, a.amqpConnOptions)
		cancel()

		if err != nil {
			numberOfAttempts++
			waitTime = waitTime * 2
			Error("Failed to connection. ", "id", a.Id(), "error", err)
		} else {
			Info("Reconnected successfully")
			a.entitiesTracker.publishers.Range(func(key, value any) bool {
				publisher := value.(*Publisher)
				// try to createSender
				err = publisher.createSender(context.Background())
				if err != nil {
					Error("Failed to createSender publisher", "ID", publisher.Id(), "error", err)
				}
				return true
			})

			break
		}
	}

}

func (a *AmqpConnection) close() {

	a.lifeCycle.SetState(&StateClosed{})
	if a.refMap != nil {
		a.refMap.Delete(a.Id())
	}
	a.entitiesTracker.CleanUp()
}

func (a *AmqpConnection) Close(ctx context.Context) error {
	err := a.management.Close(ctx)
	if err != nil {
		Error("Failed to close management", "error:", err)
	}
	err = a.Connection.Close()
	a.close()
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

func (a *AmqpConnection) Id() string {
	return a.id
}

// *** management section ***

// Management returns the management interface for the connection.
// The management interface is used to declare and delete exchanges, queues, and bindings.
func (a *AmqpConnection) Management() *AmqpManagement {
	return a.management
}

//*** end management section ***
