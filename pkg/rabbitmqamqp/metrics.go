package rabbitmqamqp

// PublishDisposition represents the outcome of a published message as reported by the broker.
type PublishDisposition int

const (
	// PublishAccepted indicates the message was accepted by the broker and will be routed.
	PublishAccepted PublishDisposition = iota
	// PublishRejected indicates the message was rejected by the broker (e.g., validation failed).
	PublishRejected
	// PublishReleased indicates the message was released by the broker (e.g., couldn't route, no queue).
	PublishReleased
)

// ConsumeDisposition represents the outcome of a consumed message as settled by the consumer.
type ConsumeDisposition int

const (
	// ConsumeAccepted indicates the consumer accepted and processed the message.
	ConsumeAccepted ConsumeDisposition = iota
	// ConsumeDiscarded indicates the consumer discarded/rejected the message (dead letter).
	ConsumeDiscarded
	// ConsumeRequeued indicates the consumer requeued the message for redelivery.
	ConsumeRequeued
)

// PublishContext contains contextual information for publish metrics,
// following OTEL semantic conventions for RabbitMQ.
type PublishContext struct {
	ServerAddress   string // Server hostname or IP (server.address)
	ServerPort      int    // Server port (server.port)
	DestinationName string // Exchange:routing_key or queue name (messaging.destination.name)
	RoutingKey      string // Routing key if applicable (messaging.rabbitmq.destination.routing_key)
	MessageID       string // Message ID if available (messaging.message.id)
}

// ConsumeContext contains contextual information for consume metrics,
// following OTEL semantic conventions for RabbitMQ.
type ConsumeContext struct {
	ServerAddress   string // Server hostname or IP (server.address)
	ServerPort      int    // Server port (server.port)
	DestinationName string // Queue name (messaging.destination.name)
	MessageID       string // Message ID if available (messaging.message.id)
}

// MetricsCollector defines the interface for collecting metrics from the AMQP client.
// All methods are fire-and-forget, non-blocking, and should never throw exceptions.
// Implementations must be thread-safe.
type MetricsCollector interface {
	// OpenConnection is called when a connection to the broker is successfully established.
	OpenConnection()

	// CloseConnection is called when a connection is permanently closed.
	// This is NOT called during connection recovery.
	CloseConnection()

	// OpenPublisher is called when a publisher resource is successfully created.
	OpenPublisher()

	// ClosePublisher is called when a publisher is closed/destroyed.
	ClosePublisher()

	// OpenConsumer is called when a consumer resource is successfully created.
	OpenConsumer()

	// CloseConsumer is called when a consumer is closed/destroyed.
	CloseConsumer()

	// Publish is called when a message is sent to the broker.
	// This is called immediately after sending, before broker acknowledgment.
	// The context contains OTEL semantic convention attributes for the publish operation.
	Publish(ctx PublishContext)

	// PublishDisposition is called when the broker acknowledges a published message.
	// The context contains OTEL semantic convention attributes for the disposition.
	PublishDisposition(disposition PublishDisposition, ctx PublishContext)

	// Consume is called when a message is delivered to a consumer.
	// This is called before invoking the application's message handler.
	// The context contains OTEL semantic convention attributes for the consume operation.
	Consume(ctx ConsumeContext)

	// ConsumeDisposition is called when the consumer settles (acknowledges) a message.
	// The context contains OTEL semantic convention attributes for the disposition.
	ConsumeDisposition(disposition ConsumeDisposition, ctx ConsumeContext)

	// WrittenBytes is called when data is written to the network socket.
	// The count parameter is the number of bytes written in this operation.
	WrittenBytes(count int64)

	// ReadBytes is called when data is read from the network socket.
	// The count parameter is the number of bytes read in this operation.
	ReadBytes(count int64)
}

// NoOpMetricsCollector is a MetricsCollector implementation that does nothing.
// It is the default metrics collector when none is specified.
// This provides zero overhead when metrics collection is disabled.
type NoOpMetricsCollector struct{}

// Ensure NoOpMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*NoOpMetricsCollector)(nil)

func (n *NoOpMetricsCollector) OpenConnection()                                           {}
func (n *NoOpMetricsCollector) CloseConnection()                                          {}
func (n *NoOpMetricsCollector) OpenPublisher()                                            {}
func (n *NoOpMetricsCollector) ClosePublisher()                                           {}
func (n *NoOpMetricsCollector) OpenConsumer()                                             {}
func (n *NoOpMetricsCollector) CloseConsumer()                                            {}
func (n *NoOpMetricsCollector) Publish(_ PublishContext)                                  {}
func (n *NoOpMetricsCollector) PublishDisposition(_ PublishDisposition, _ PublishContext) {}
func (n *NoOpMetricsCollector) Consume(_ ConsumeContext)                                  {}
func (n *NoOpMetricsCollector) ConsumeDisposition(_ ConsumeDisposition, _ ConsumeContext) {}
func (n *NoOpMetricsCollector) WrittenBytes(_ int64)                                      {}
func (n *NoOpMetricsCollector) ReadBytes(_ int64)                                         {}

// defaultMetricsCollector is the singleton NoOpMetricsCollector instance used as default.
var defaultMetricsCollector MetricsCollector = &NoOpMetricsCollector{}

// DefaultMetricsCollector returns the default no-op metrics collector.
func DefaultMetricsCollector() MetricsCollector {
	return defaultMetricsCollector
}
