package rabbitmqamqp

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// DefaultMetricsPrefix is the default prefix for all metrics.
	DefaultMetricsPrefix = "rabbitmq.amqp"
)

// OTELMetricsCollector is a MetricsCollector implementation using OpenTelemetry.
// It uses only the OTEL API packages (not the SDK), making it suitable for library use.
// The SDK is expected to be configured by the application using this library.
type OTELMetricsCollector struct {
	// Gauges (using UpDownCounter for gauge semantics)
	connections metric.Int64UpDownCounter
	publishers  metric.Int64UpDownCounter
	consumers   metric.Int64UpDownCounter

	// Counters
	published         metric.Int64Counter
	publishedAccepted metric.Int64Counter
	publishedRejected metric.Int64Counter
	publishedReleased metric.Int64Counter
	consumed          metric.Int64Counter
	consumedAccepted  metric.Int64Counter
	consumedDiscarded metric.Int64Counter
	consumedRequeued  metric.Int64Counter
	writtenBytes      metric.Int64Counter
	readBytes         metric.Int64Counter
}

// Ensure OTELMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*OTELMetricsCollector)(nil)

// NewOTELMetricsCollector creates a new OTELMetricsCollector with the given MeterProvider and prefix.
// If prefix is empty, DefaultMetricsPrefix ("rabbitmq.amqp") is used.
// The MeterProvider should be configured by the application using this library.
func NewOTELMetricsCollector(meterProvider metric.MeterProvider, prefix string) (*OTELMetricsCollector, error) {
	if prefix == "" {
		prefix = DefaultMetricsPrefix
	}

	meter := meterProvider.Meter("github.com/rabbitmq/rabbitmq-amqp-go-client")

	collector := &OTELMetricsCollector{}
	var err error

	// Create gauge metrics (using UpDownCounter for gauge semantics)
	collector.connections, err = meter.Int64UpDownCounter(
		prefix+".connections",
		metric.WithDescription("Current number of open connections"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return nil, err
	}

	collector.publishers, err = meter.Int64UpDownCounter(
		prefix+".publishers",
		metric.WithDescription("Current number of open publishers"),
		metric.WithUnit("{publisher}"),
	)
	if err != nil {
		return nil, err
	}

	collector.consumers, err = meter.Int64UpDownCounter(
		prefix+".consumers",
		metric.WithDescription("Current number of open consumers"),
		metric.WithUnit("{consumer}"),
	)
	if err != nil {
		return nil, err
	}

	// Create counter metrics for publishing
	collector.published, err = meter.Int64Counter(
		prefix+".published",
		metric.WithDescription("Total number of messages published"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.publishedAccepted, err = meter.Int64Counter(
		prefix+".published.accepted",
		metric.WithDescription("Total number of messages accepted by broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.publishedRejected, err = meter.Int64Counter(
		prefix+".published.rejected",
		metric.WithDescription("Total number of messages rejected by broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.publishedReleased, err = meter.Int64Counter(
		prefix+".published.released",
		metric.WithDescription("Total number of messages released by broker"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	// Create counter metrics for consuming
	collector.consumed, err = meter.Int64Counter(
		prefix+".consumed",
		metric.WithDescription("Total number of messages delivered to consumers"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.consumedAccepted, err = meter.Int64Counter(
		prefix+".consumed.accepted",
		metric.WithDescription("Total number of messages accepted by consumer"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.consumedDiscarded, err = meter.Int64Counter(
		prefix+".consumed.discarded",
		metric.WithDescription("Total number of messages discarded by consumer"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	collector.consumedRequeued, err = meter.Int64Counter(
		prefix+".consumed.requeued",
		metric.WithDescription("Total number of messages requeued by consumer"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	// Create counter metrics for network I/O
	collector.writtenBytes, err = meter.Int64Counter(
		prefix+".written_bytes",
		metric.WithDescription("Total bytes written to network"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	collector.readBytes, err = meter.Int64Counter(
		prefix+".read_bytes",
		metric.WithDescription("Total bytes read from network"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	return collector, nil
}

// OpenConnection increments the connections gauge.
func (o *OTELMetricsCollector) OpenConnection() {
	o.connections.Add(context.Background(), 1)
}

// CloseConnection decrements the connections gauge.
func (o *OTELMetricsCollector) CloseConnection() {
	o.connections.Add(context.Background(), -1)
}

// OpenPublisher increments the publishers gauge.
func (o *OTELMetricsCollector) OpenPublisher() {
	o.publishers.Add(context.Background(), 1)
}

// ClosePublisher decrements the publishers gauge.
func (o *OTELMetricsCollector) ClosePublisher() {
	o.publishers.Add(context.Background(), -1)
}

// OpenConsumer increments the consumers gauge.
func (o *OTELMetricsCollector) OpenConsumer() {
	o.consumers.Add(context.Background(), 1)
}

// CloseConsumer decrements the consumers gauge.
func (o *OTELMetricsCollector) CloseConsumer() {
	o.consumers.Add(context.Background(), -1)
}

// WrittenBytes increments the written bytes counter.
func (o *OTELMetricsCollector) WrittenBytes(count int64) {
	o.writtenBytes.Add(context.Background(), count)
}

// ReadBytes increments the read bytes counter.
func (o *OTELMetricsCollector) ReadBytes(count int64) {
	o.readBytes.Add(context.Background(), count)
}

// buildBaseAttributes creates the common OTEL semantic convention attributes
// for RabbitMQ messaging operations.
func buildBaseAttributes(serverAddress string, serverPort int, destinationName string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		MessagingSystemKey.String(MessagingSystemRabbitMQ),
	}
	if destinationName != "" {
		attrs = append(attrs, MessagingDestinationNameKey.String(destinationName))
	}
	if serverAddress != "" {
		attrs = append(attrs, ServerAddressKey.String(serverAddress))
	}
	if serverPort > 0 {
		attrs = append(attrs, ServerPortKey.Int(serverPort))
	}
	return attrs
}

// Publish increments the published counter with OTEL semantic convention attributes.
func (o *OTELMetricsCollector) Publish(ctx PublishContext) {
	attrs := buildBaseAttributes(ctx.ServerAddress, ctx.ServerPort, ctx.DestinationName)
	attrs = append(attrs,
		MessagingOperationNameKey.String(OperationNameSend),
		MessagingOperationTypeKey.String(OperationTypeSend),
	)
	if ctx.RoutingKey != "" {
		attrs = append(attrs, MessagingRabbitMQRoutingKeyKey.String(ctx.RoutingKey))
	}
	if ctx.MessageID != "" {
		attrs = append(attrs, MessagingMessageIDKey.String(ctx.MessageID))
	}

	o.published.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// PublishDisposition increments the appropriate disposition counter with OTEL semantic convention attributes.
func (o *OTELMetricsCollector) PublishDisposition(disposition PublishDisposition, ctx PublishContext) {
	attrs := buildBaseAttributes(ctx.ServerAddress, ctx.ServerPort, ctx.DestinationName)
	attrs = append(attrs, MessagingOperationTypeKey.String(OperationTypeSettle))

	if ctx.RoutingKey != "" {
		attrs = append(attrs, MessagingRabbitMQRoutingKeyKey.String(ctx.RoutingKey))
	}
	if ctx.MessageID != "" {
		attrs = append(attrs, MessagingMessageIDKey.String(ctx.MessageID))
	}

	switch disposition {
	case PublishAccepted:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameAck))
		o.publishedAccepted.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	case PublishRejected:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameNack))
		o.publishedRejected.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	case PublishReleased:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameNack))
		o.publishedReleased.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	}
}

// Consume increments the consumed counter with OTEL semantic convention attributes.
func (o *OTELMetricsCollector) Consume(ctx ConsumeContext) {
	attrs := buildBaseAttributes(ctx.ServerAddress, ctx.ServerPort, ctx.DestinationName)
	attrs = append(attrs,
		MessagingOperationNameKey.String(OperationNameReceive),
		MessagingOperationTypeKey.String(OperationTypeReceive),
	)
	if ctx.MessageID != "" {
		attrs = append(attrs, MessagingMessageIDKey.String(ctx.MessageID))
	}

	o.consumed.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// ConsumeDisposition increments the appropriate disposition counter with OTEL semantic convention attributes.
func (o *OTELMetricsCollector) ConsumeDisposition(disposition ConsumeDisposition, ctx ConsumeContext) {
	attrs := buildBaseAttributes(ctx.ServerAddress, ctx.ServerPort, ctx.DestinationName)
	attrs = append(attrs, MessagingOperationTypeKey.String(OperationTypeSettle))

	if ctx.MessageID != "" {
		attrs = append(attrs, MessagingMessageIDKey.String(ctx.MessageID))
	}

	switch disposition {
	case ConsumeAccepted:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameAck))
		o.consumedAccepted.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	case ConsumeDiscarded:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameNack))
		o.consumedDiscarded.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	case ConsumeRequeued:
		attrs = append(attrs, MessagingOperationNameKey.String(OperationNameRequeue))
		o.consumedRequeued.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	}
}
