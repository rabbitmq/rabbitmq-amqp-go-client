package rabbitmqamqp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Azure/go-amqp"
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

/*
Environment is the main entry point for the library. It is responsible for managing the connections to the RabbitMQ server.
It keeps track of the connections and the endpoints,and provides methods to create new connections and close all connections.
*/
type Environment struct {
	connections      sync.Map
	endPoints        []Endpoint
	EndPointStrategy TEndPointStrategy
	nextConnectionId int32
}

/*
NewEnvironment creates a new environment with the given address and options.
The address is the AMQP connection string, and the options are the connection options.
The default strategy is StrategyRandom, which picks a random endpoint from the list of endpoints when creating a new connection.
If you want to use a different strategy, you can use NewClusterEnvironmentWithStrategy.
*/
func NewEnvironment(address string, options *AmqpConnOptions) *Environment {
	return NewClusterEnvironmentWithStrategy([]Endpoint{{Address: address, Options: options}}, StrategyRandom)
}

/*
NewClusterEnvironment creates a new environment with the given endpoints.
The default strategy is StrategyRandom, which picks a random endpoint from the list of endpoints when creating a new connection.
If you want to use a different strategy, you can use NewClusterEnvironmentWithStrategy.
*/

func NewClusterEnvironment(endPoints []Endpoint) *Environment {
	return NewClusterEnvironmentWithStrategy(endPoints, StrategyRandom)
}

/*
NewClusterEnvironmentWithStrategy creates a new environment with the given endpoints and strategy.
The strategy is used to determine how the environment picks an endpoint from the list of endpoints when creating a new connection.
The default strategy is StrategyRandom, which picks a random endpoint from the list of endpoints.
The other strategy is StrategySequential, which picks the endpoints in order.
*/
func NewClusterEnvironmentWithStrategy(endPoints []Endpoint, strategy TEndPointStrategy) *Environment {
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
		var cloned *AmqpConnOptions
		if addr.Options != nil {
			cloned = addr.Options.Clone()
		}
		connection, err := Dial(ctx, addr.Address, cloned)
		if err != nil {
			Error("Failed to open connection", "url", ExtractWithoutPassword(addr.Address), "error", err)
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
