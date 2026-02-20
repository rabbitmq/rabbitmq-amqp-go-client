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
	metricsCollector MetricsCollector
}

// environmentOptions holds optional configuration for Environment.
type environmentOptions struct {
	strategy         TEndPointStrategy
	metricsCollector MetricsCollector
}

// EnvironmentOption is a function to configure the Environment. See WithStrategy and WithMetricsCollector for more details.
type EnvironmentOption func(*environmentOptions)

// WithStrategy sets the endpoint selection strategy for the Environment.
func WithStrategy(strategy TEndPointStrategy) EnvironmentOption {
	return func(opts *environmentOptions) {
		opts.strategy = strategy
	}
}

// WithMetricsCollector sets the metrics collector for the Environment.
func WithMetricsCollector(collector MetricsCollector) EnvironmentOption {
	return func(opts *environmentOptions) {
		opts.metricsCollector = collector
	}
}

// applyEnvironmentOptions applies the given options to environmentOptions with defaults.
func applyEnvironmentOptions(opts ...EnvironmentOption) *environmentOptions {
	envOpts := &environmentOptions{
		strategy:         StrategyRandom,
		metricsCollector: DefaultMetricsCollector(),
	}
	for _, opt := range opts {
		opt(envOpts)
	}
	return envOpts
}

/*
NewEnvironment creates a new environment with the given address and options.
The address is the AMQP connection string, and the options are the connection options.
Optional environment configuration can be passed using functional options.
*/
func NewEnvironment(address string, connOptions *AmqpConnOptions, opts ...EnvironmentOption) *Environment {
	envOpts := applyEnvironmentOptions(opts...)
	return &Environment{
		connections:      sync.Map{},
		endPoints:        []Endpoint{{Address: address, Options: connOptions}},
		EndPointStrategy: envOpts.strategy,
		nextConnectionId: 0,
		metricsCollector: envOpts.metricsCollector,
	}
}

/*
NewClusterEnvironment creates a new environment with the given endpoints.
Optional environment configuration can be passed using functional options.
*/
func NewClusterEnvironment(endPoints []Endpoint, opts ...EnvironmentOption) *Environment {
	envOpts := applyEnvironmentOptions(opts...)
	return &Environment{
		connections:      sync.Map{},
		endPoints:        endPoints,
		EndPointStrategy: envOpts.strategy,
		nextConnectionId: 0,
		metricsCollector: envOpts.metricsCollector,
	}
}

/*
NewClusterEnvironmentWithStrategy creates a new environment with the given endpoints and strategy.
This function is provided for backward compatibility. Consider using NewClusterEnvironment with WithStrategy option instead.
*/
func NewClusterEnvironmentWithStrategy(endPoints []Endpoint, strategy TEndPointStrategy) *Environment {
	return &Environment{
		connections:      sync.Map{},
		endPoints:        endPoints,
		EndPointStrategy: strategy,
		nextConnectionId: 0,
		metricsCollector: DefaultMetricsCollector(),
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
		connection, err := dialWithMetrics(ctx, addr.Address, cloned, e.metricsCollector)
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
