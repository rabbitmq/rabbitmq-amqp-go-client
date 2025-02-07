package rabbitmq_amqp

import (
	"context"
	"fmt"
	"sync"
)

type Environment struct {
	connections sync.Map
	addresses   []string
	connOptions *AmqpConnOptions
}

func NewEnvironment(addresses []string, connOptions *AmqpConnOptions) *Environment {
	return &Environment{
		connections: sync.Map{},
		addresses:   addresses,
		connOptions: connOptions,
	}
}

// NewConnection get a new connection from the environment.
// If the connection id is provided, it will be used as the connection id.
// If the connection id is not provided, a new connection id will be generated.
// The connection id is unique in the environment.
// The Environment will keep track of the connection and close it when the environment is closed.
func (e *Environment) NewConnection(ctx context.Context, args ...string) (*AmqpConnection, error) {
	if len(args) > 0 && len(args[0]) > 0 {
		// check if connection already exists
		if _, ok := e.connections.Load(args[0]); ok {
			return nil, fmt.Errorf("connection with id %s already exists", args[0])
		}
	}

	connection, err := Dial(ctx, e.addresses, e.connOptions, args...)
	if err != nil {
		return nil, err
	}
	e.connections.Store(connection.Id(), connection)
	connection.refMap = &e.connections
	return connection, nil
}

// Connections gets the active connections in the environment

func (e *Environment) Connections() []*AmqpConnection {
	connections := make([]*AmqpConnection, 0)
	e.connections.Range(func(key, value interface{}) bool {
		connections = append(connections, value.(*AmqpConnection))
		return true
	})
	return connections
}

// CloseConnections closes all the connections in the environment with all the publishers and consumers.
func (e *Environment) CloseConnections(ctx context.Context) error {
	var err error
	e.connections.Range(func(key, value any) bool {
		connection := value.(*AmqpConnection)
		if cerr := connection.Close(ctx); cerr != nil {
			err = cerr
		}
		return true
	})
	return err
}
