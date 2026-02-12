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

type Environment struct {
	connections      sync.Map
	endPoints        []Endpoint
	EndPointStrategy TEndPointStrategy
	nextConnectionId int32
	metricsCollector MetricsCollector
}

// environmentOptions holds internal configuration for the Environment.
type environmentOptions struct {
	strategy         TEndPointStrategy
	metricsCollector MetricsCollector
}

// EnvironmentOption configures an Environment.
// Use the With* functions to create options.
type EnvironmentOption func(*environmentOptions)

// WithStrategy sets the endpoint selection strategy.
// Default is StrategyRandom.
func WithStrategy(strategy TEndPointStrategy) EnvironmentOption {
	return func(o *environmentOptions) {
		o.strategy = strategy
	}
}

// WithMetricsCollector sets the metrics collector.
// Default is NoOpMetricsCollector.
func WithMetricsCollector(collector MetricsCollector) EnvironmentOption {
	return func(o *environmentOptions) {
		o.metricsCollector = collector
	}
}

// NewEnvironment creates a new single-endpoint Environment with optional configuration.
// Options can be provided using the With* functions (e.g., WithStrategy, WithMetricsCollector).
func NewEnvironment(address string, connOptions *AmqpConnOptions, opts ...EnvironmentOption) *Environment {
	return NewClusterEnvironment([]Endpoint{{Address: address, Options: connOptions}}, opts...)
}

// NewClusterEnvironment creates a new multi-endpoint Environment with optional configuration.
// Options can be provided using the With* functions (e.g., WithStrategy, WithMetricsCollector).
func NewClusterEnvironment(endPoints []Endpoint, opts ...EnvironmentOption) *Environment {
	// Start with defaults
	options := &environmentOptions{
		strategy:         StrategyRandom,
		metricsCollector: nil,
	}

	// Apply all options
	for _, opt := range opts {
		opt(options)
	}

	if options.metricsCollector == nil {
		options.metricsCollector = DefaultMetricsCollector()
	}

	return &Environment{
		connections:      sync.Map{},
		endPoints:        endPoints,
		EndPointStrategy: options.strategy,
		nextConnectionId: 0,
		metricsCollector: options.metricsCollector,
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
