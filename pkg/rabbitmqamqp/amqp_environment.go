package rabbitmqamqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"sync"
)

type Endpoint struct {
	Address string
	Options *AmqpConnOptions
}

func (e *Endpoint) toAddress() string {
	return e.Address
}

func (e *Endpoint) options() *AmqpConnOptions {
	return e.Options
}

func DefaultEndpoints() []*Endpoint {
	ep := &Endpoint{
		Address: "amqp://",
		Options: &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
		},
	}

	return []*Endpoint{ep}

}

type Environment struct {
	connections sync.Map
	endPoints   []*Endpoint
}

func NewEnvironment(endPoints []*Endpoint) *Environment {
	return &Environment{
		connections: sync.Map{},
		endPoints:   endPoints,
	}
}

// NewConnection get a new connection from the environment.
// If the connection id is provided, it will be used as the connection id.
// If the connection id is not provided, a new connection id will be generated.
// The connection id is unique in the environment.
// The Environment will keep track of the connection and close it when the environment is closed.
func (e *Environment) NewConnection(ctx context.Context, id string) (*AmqpConnection, error) {
	if id == "" {
		id = fmt.Sprintf("connection-%s", uuid.New().String())
	}
	if _, ok := e.connections.Load(id); ok {
		return nil, fmt.Errorf("connection with id %s already exists", id)
	}

	tmp := make([]*Endpoint, len(e.endPoints))
	copy(tmp, e.endPoints)
	lastError := error(nil)
	for len(tmp) > 0 {
		idx := random(len(tmp))
		addr := tmp[idx]
		// remove the index from the tmp list
		tmp = append(tmp[:idx], tmp[idx+1:]...)

		connection, err := Dial(ctx, addr.toAddress(), addr.options())
		if err != nil {
			Error("Failed to open connection", ExtractWithoutPassword(addr.toAddress()), err)
			lastError = err
			continue
		}

		connection.amqpConnOptions.Id = id
		e.connections.Store(connection.Id(), connection)
		connection.refMap = &e.connections
		return connection, nil
	}
	return nil, fmt.Errorf("fail to open connection. Last error: %w", lastError)
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
