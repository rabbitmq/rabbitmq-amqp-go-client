package rabbitmqamqp

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"sync/atomic"
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
	properties        map[string]any
	featuresAvailable *featuresAvailable

	azureConnection *amqp.Conn
	id              string
	management      *AmqpManagement
	lifeCycle       *LifeCycle
	amqpConnOptions *AmqpConnOptions
	session         *amqp.Session
	refMap          *sync.Map
	entitiesTracker *entitiesTracker
}

func (a *AmqpConnection) Properties() map[string]any {
	return a.properties

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
func (a *AmqpConnection) NewConsumer(ctx context.Context, queueName string, options ConsumerOptions) (*Consumer, error) {
	destination := &QueueAddress{
		Queue: queueName,
	}

	destinationAdd, err := destination.toAddress()
	if err != nil {
		return nil, err
	}

	return newConsumer(ctx, a, destinationAdd, options)
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

	// validate the RecoveryConfiguration options
	if connOptions.RecoveryConfiguration.MaxReconnectAttempts <= 0 && connOptions.RecoveryConfiguration.ActiveRecovery {
		return nil, fmt.Errorf("MaxReconnectAttempts should be greater than 0")
	}
	if connOptions.RecoveryConfiguration.BackOffReconnectInterval <= 1*time.Second && connOptions.RecoveryConfiguration.ActiveRecovery {
		return nil, fmt.Errorf("BackOffReconnectInterval should be greater than 1 second")
	}

	// create the connection

	conn := &AmqpConnection{
		management:        NewAmqpManagement(),
		lifeCycle:         NewLifeCycle(),
		amqpConnOptions:   connOptions,
		entitiesTracker:   newEntitiesTracker(),
		featuresAvailable: newFeaturesAvailable(),
	}
	tmp := make([]string, len(addresses))
	copy(tmp, addresses)

	err := conn.open(ctx, addresses, connOptions, args...)
	if err != nil {
		return nil, err
	}
	conn.amqpConnOptions = connOptions
	conn.amqpConnOptions.addresses = addresses
	conn.lifeCycle.SetState(&StateOpen{})
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
	var azureConnection *amqp.Conn
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
		azureConnection, err = amqp.Dial(ctx, addr, amqpLiteConnOptions)
		if err != nil {
			Error("Failed to open connection", ExtractWithoutPassword(addr), err)
			continue
		}
		a.properties = azureConnection.Properties()
		err = a.featuresAvailable.ParseProperties(a.properties)
		if err != nil {
			Warn("Validate properties Error.", ExtractWithoutPassword(addr), err)
		}

		if !a.featuresAvailable.is4OrMore {
			Warn("The server version is less than 4.0.0", ExtractWithoutPassword(addr))
		}

		if !a.featuresAvailable.isRabbitMQ {
			Warn("The server is not RabbitMQ", ExtractWithoutPassword(addr))
		}

		Debug("Connected to", ExtractWithoutPassword(addr))
		break
	}
	if azureConnection == nil {
		return fmt.Errorf("failed to connect to any of the provided addresses")
	}

	if len(args) > 0 {
		a.id = args[0]
	} else {
		a.id = uuid.New().String()
	}

	a.azureConnection = azureConnection
	var err error
	a.session, err = a.azureConnection.NewSession(ctx, nil)
	go func() {
		<-azureConnection.Done()
		{
			a.lifeCycle.SetState(&StateClosed{error: azureConnection.Err()})
			if azureConnection.Err() != nil {
				Error("connection closed unexpectedly", "error", azureConnection.Err())
				a.maybeReconnect()

				return
			}
			Debug("connection closed successfully")
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

	return nil
}
func (a *AmqpConnection) maybeReconnect() {

	if !a.amqpConnOptions.RecoveryConfiguration.ActiveRecovery {
		Info("Recovery is disabled, closing connection")
		return
	}
	a.lifeCycle.SetState(&StateReconnecting{})
	// Add exponential backoff with jitter
	baseDelay := a.amqpConnOptions.RecoveryConfiguration.BackOffReconnectInterval
	maxDelay := 1 * time.Minute

	for attempt := 1; attempt <= a.amqpConnOptions.RecoveryConfiguration.MaxReconnectAttempts; attempt++ {

		///wait for before reconnecting
		// add some random milliseconds to the wait time to avoid thundering herd
		// the random time is between 0 and 500 milliseconds
		// Calculate delay with exponential backoff and jitter
		jitter := time.Duration(rand.Intn(500)) * time.Millisecond
		delay := baseDelay + jitter
		if delay > maxDelay {
			delay = maxDelay
		}

		Info("Attempting reconnection", "attempt", attempt, "delay", delay)
		time.Sleep(delay)
		// context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		// try to createSender
		err := a.open(ctx, a.amqpConnOptions.addresses, a.amqpConnOptions)
		cancel()

		if err == nil {
			a.restartEntities()
			a.lifeCycle.SetState(&StateOpen{})
			return
		}
		baseDelay *= 2
		Error("Reconnection attempt failed", "attempt", attempt, "error", err)
	}

}

// restartEntities attempts to restart all publishers and consumers after a reconnection
func (a *AmqpConnection) restartEntities() {
	var publisherFails, consumerFails int32

	// Restart publishers
	a.entitiesTracker.publishers.Range(func(key, value any) bool {
		publisher := value.(*Publisher)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := publisher.createSender(ctx); err != nil {
			atomic.AddInt32(&publisherFails, 1)
			Error("Failed to restart publisher", "ID", publisher.Id(), "error", err)
		}
		return true
	})

	// Restart consumers
	a.entitiesTracker.consumers.Range(func(key, value any) bool {
		consumer := value.(*Consumer)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := consumer.createReceiver(ctx); err != nil {
			atomic.AddInt32(&consumerFails, 1)
			Error("Failed to restart consumer", "ID", consumer.Id(), "error", err)
		}
		return true
	})

	Info("Entity restart complete",
		"publisherFails", publisherFails,
		"consumerFails", consumerFails)
}

func (a *AmqpConnection) close() {
	if a.refMap != nil {
		a.refMap.Delete(a.Id())
	}
	a.entitiesTracker.CleanUp()
}

/*
Close closes the connection to the AMQP 1.0 server and the management interface.
All the publishers and consumers are closed as well.
*/
func (a *AmqpConnection) Close(ctx context.Context) error {
	// the status closed (lifeCycle.SetState(&StateClosed{error: nil})) is not set here
	// it is set in the connection.Done() channel
	// the channel is called anyway
	// see the open(...) function with a.lifeCycle.SetState(&StateClosed{error: connection.Err()})

	err := a.management.Close(ctx)
	if err != nil {
		Error("Failed to close management", "error:", err)
	}
	err = a.azureConnection.Close()
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
