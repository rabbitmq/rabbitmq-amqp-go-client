package rabbitmqamqp

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"sync"
	"sync/atomic"
)

type TEndPointStrategy int

const (
	StrategyRandom     TEndPointStrategy = iota
	StrategySequential TEndPointStrategy = iota
)

type Endpoint struct {
	Address string
	Options *AmqpConnOptions
}

func DefaultEndpoints() []Endpoint {
	ep := Endpoint{
		Address: "amqp://",
		Options: &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
		},
	}

	return []Endpoint{ep}
}

type Environment struct {
	connections      sync.Map
	endPoints        []Endpoint
	EndPointStrategy TEndPointStrategy
	nextConnectionId int32
}

func NewEnvironment(endPoints []Endpoint) *Environment {
	return NewEnvironmentWithStrategy(endPoints, StrategyRandom)
}

func NewEnvironmentWithStrategy(endPoints []Endpoint, strategy TEndPointStrategy) *Environment {
	return &Environment{
		connections:      sync.Map{},
		endPoints:        endPoints,
		EndPointStrategy: strategy,
		nextConnectionId: 0,
	}
}

// NewConnection get a new connection from the environment.
// It picks an endpoint from the list of endpoints, based on EndPointStrategy, and tries to open a connection.
// It fails if all the endpoints are not reachable.
// The Environment will keep track of the connection and close it when the environment is closed.
func (e *Environment) NewConnection(ctx context.Context) (*AmqpConnection, error) {

	tmp := make([]Endpoint, len(e.endPoints))
	copy(tmp, e.endPoints)
	lastError := error(nil)
	for len(tmp) > 0 {
		idx := 0

		switch e.EndPointStrategy {
		case StrategyRandom:
			idx = random(len(tmp))
		case StrategySequential:
			idx = 0
		}

		addr := tmp[idx]
		// remove the index from the tmp list
		tmp = append(tmp[:idx], tmp[idx+1:]...)
		if addr.Options == nil {
			addr.Options = &AmqpConnOptions{
				SASLType: amqp.SASLTypeAnonymous(),
			}
		}
		connection, err := Dial(ctx, addr.Address, addr.Options.Clone())
		if err != nil {
			Error("Failed to open connection", ExtractWithoutPassword(addr.Address), err)
			lastError = err
			continue
		}

		// here we use it to make each connection unique
		atomic.AddInt32(&e.nextConnectionId, 1)
		connection.amqpConnOptions.Id = fmt.Sprintf("%s_%d", connection.amqpConnOptions.Id, e.nextConnectionId)
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
