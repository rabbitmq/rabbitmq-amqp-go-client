package otelmetrics_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestMetricsEndToEndBasicFlow performs a complete publish/consume cycle
// and verifies all metrics are correctly recorded.
func TestMetricsEndToEndBasicFlow(t *testing.T) {
	// Skip if RabbitMQ is not available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Setup OTEL with ManualReader for metric assertions
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("Failed to shutdown meter provider: %v", err)
		}
	})

	// Create OTEL metrics collector
	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.e2e")
	if err != nil {
		t.Fatalf("Failed to create OTEL metrics collector: %v", err)
	}

	// Create environment with metrics
	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil,
		rmq.WithMetricsCollector(collector))

	// Open connection
	connection, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v (ensure RabbitMQ is running at localhost:5672)", err)
	}

	// Declare a test queue
	queueName := fmt.Sprintf("test-e2e-metrics-%d", time.Now().UnixNano())
	management := connection.Management()
	_, err = management.DeclareQueue(t.Context(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	t.Cleanup(func() {
		// Cleanup queue
		if err := management.DeleteQueue(context.Background(), queueName); err != nil {
			t.Logf("Warning: Failed to delete queue: %v", err)
		}
	})

	// Create publisher
	publisher, err := connection.NewPublisher(t.Context(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	// Create consumer
	consumer, err := connection.NewConsumer(t.Context(), queueName, nil)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Publish messages
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("Test message %d", i+1)))
		result, err := publisher.Publish(t.Context(), msg)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i+1, err)
		}
		// Verify message was accepted
		if _, ok := result.Outcome.(*rmq.StateAccepted); !ok {
			t.Errorf("Message %d was not accepted: %v", i+1, result.Outcome)
		}
	}

	// Consume and accept messages
	consumedCount := 0
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	for consumedCount < messageCount {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			t.Fatalf("Failed to receive message: %v", err)
		}
		err = delivery.Accept(t.Context())
		if err != nil {
			t.Fatalf("Failed to accept message: %v", err)
		}
		consumedCount++
	}

	if consumedCount != messageCount {
		t.Errorf("Expected to consume %d messages, got %d", messageCount, consumedCount)
	}

	// Close resources
	if err := consumer.Close(t.Context()); err != nil {
		t.Errorf("Failed to close consumer: %v", err)
	}
	if err := publisher.Close(t.Context()); err != nil {
		t.Errorf("Failed to close publisher: %v", err)
	}
	if err := connection.Close(t.Context()); err != nil {
		t.Errorf("Failed to close connection: %v", err)
	}

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(t.Context(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Extract metrics into a map for easier verification
	metrics := extractMetrics(rm)

	// Assert metrics
	t.Run("connections gauge", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.connections", 0, "connection should be closed")
	})

	t.Run("publishers gauge", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.publishers", 0, "publisher should be closed")
	})

	t.Run("consumers gauge", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.consumers", 0, "consumer should be closed")
	})

	t.Run("published counter", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.published", int64(messageCount), "should have published %d messages", messageCount)
	})

	t.Run("published.accepted counter", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.published.accepted", int64(messageCount), "all messages should be accepted by broker")
	})

	t.Run("consumed counter", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.consumed", int64(messageCount), "should have consumed %d messages", messageCount)
	})

	t.Run("consumed.accepted counter", func(t *testing.T) {
		assertMetric(t, metrics, "test.e2e.consumed.accepted", int64(messageCount), "all messages should be accepted by consumer")
	})
}

// TestMetricsEndToEndPartialClose verifies metrics when only some resources are closed.
func TestMetricsEndToEndPartialClose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		meterProvider.Shutdown(context.Background())
	})

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.e2e.partial")
	if err != nil {
		t.Fatalf("Failed to create OTEL metrics collector: %v", err)
	}

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil,
		rmq.WithMetricsCollector(collector))

	connection, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	queueName := fmt.Sprintf("test-e2e-partial-%d", time.Now().UnixNano())
	_, err = connection.Management().DeclareQueue(t.Context(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	t.Cleanup(func() {
		connection.Management().DeleteQueue(context.Background(), queueName)
	})

	publisher, err := connection.NewPublisher(t.Context(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	consumer, err := connection.NewConsumer(t.Context(), queueName, nil)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Close only the publisher, leave consumer and connection open
	if err := publisher.Close(t.Context()); err != nil {
		t.Errorf("Failed to close publisher: %v", err)
	}

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(t.Context(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	metrics := extractMetrics(rm)

	// Connection should still be open
	assertMetric(t, metrics, "test.e2e.partial.connections", 1, "connection should still be open")

	// Publisher should be closed
	assertMetric(t, metrics, "test.e2e.partial.publishers", 0, "publisher should be closed")

	// Consumer should still be open
	assertMetric(t, metrics, "test.e2e.partial.consumers", 1, "consumer should still be open")

	// Cleanup
	t.Cleanup(func() {
		consumer.Close(context.Background())
		connection.Close(context.Background())
	})
}

// TestMetricsEndToEndMultipleConnections verifies metrics with multiple concurrent resources.
func TestMetricsEndToEndMultipleConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		meterProvider.Shutdown(context.Background())
	})

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.e2e.multi")
	if err != nil {
		t.Fatalf("Failed to create OTEL metrics collector: %v", err)
	}

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil,
		rmq.WithMetricsCollector(collector))

	// Open 3 connections
	conn1, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to create connection 1: %v", err)
	}
	t.Cleanup(func() {
		conn1.Close(context.Background())
	})

	conn2, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to create connection 2: %v", err)
	}
	t.Cleanup(func() {
		conn2.Close(context.Background())
	})

	conn3, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to create connection 3: %v", err)
	}

	// Collect metrics - 3 connections open, 1 closed
	var rm metricdata.ResourceMetrics
	err = reader.Collect(t.Context(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	metrics := extractMetrics(rm)

	// All 3 connections should be open
	assertMetric(t, metrics, "test.e2e.multi.connections", 3, "should have 3 open connections")

	// Close one connection
	if err := conn3.Close(t.Context()); err != nil {
		t.Errorf("Failed to close connection 3: %v", err)
	}

	// Collect metrics again
	rm = metricdata.ResourceMetrics{}
	err = reader.Collect(t.Context(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	metrics = extractMetrics(rm)

	// Now only 2 connections should be open
	assertMetric(t, metrics, "test.e2e.multi.connections", 2, "should have 2 open connections")
}

// extractMetrics converts metricdata.ResourceMetrics into a simple map for easier testing.
func extractMetrics(rm metricdata.ResourceMetrics) map[string]int64 {
	metrics := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					metrics[m.Name] = dp.Value
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					metrics[m.Name] = dp.Value
				}
			}
		}
	}
	return metrics
}

// assertMetric checks if a metric has the expected value.
func assertMetric(t *testing.T, metrics map[string]int64, name string, expected int64, msgFmt string, args ...interface{}) {
	t.Helper()
	actual, ok := metrics[name]
	if !ok {
		t.Errorf("Metric %s not found", name)
		return
	}
	if actual != expected {
		msg := fmt.Sprintf(msgFmt, args...)
		t.Errorf("Metric %s: expected %d, got %d (%s)", name, expected, actual, msg)
	}
}

// TestMetricsEndToEndWithSemanticConventions verifies that OTEL semantic convention
// attributes are correctly recorded during a real publish/consume cycle.
func TestMetricsEndToEndWithSemanticConventions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			t.Errorf("Failed to shutdown meter provider: %v", err)
		}
	})

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.semconv")
	if err != nil {
		t.Fatalf("Failed to create OTEL metrics collector: %v", err)
	}

	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil,
		rmq.WithMetricsCollector(collector))

	connection, err := env.NewConnection(t.Context())
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	t.Cleanup(func() {
		connection.Close(context.Background())
	})

	queueName := fmt.Sprintf("test-semconv-%d", time.Now().UnixNano())
	_, err = connection.Management().DeclareQueue(t.Context(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	t.Cleanup(func() {
		connection.Management().DeleteQueue(context.Background(), queueName)
	})

	// Create publisher and publish a message
	publisher, err := connection.NewPublisher(t.Context(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}
	t.Cleanup(func() {
		publisher.Close(context.Background())
	})

	msg := rmq.NewMessage([]byte("test message with semconv"))
	_, err = publisher.Publish(t.Context(), msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Create consumer and consume the message
	consumer, err := connection.NewConsumer(t.Context(), queueName, nil)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	t.Cleanup(func() {
		consumer.Close(context.Background())
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	delivery, err := consumer.Receive(ctx)
	if err != nil {
		t.Fatalf("Failed to receive message: %v", err)
	}
	err = delivery.Accept(t.Context())
	if err != nil {
		t.Fatalf("Failed to accept message: %v", err)
	}

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(t.Context(), &rm)
	if err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	// Extract metrics with data points for attribute checking
	metricsWithAttrs := extractMetricsWithDataPoints(rm)

	// Verify published metric has OTEL semantic convention attributes
	t.Run("published metric has semantic convention attributes", func(t *testing.T) {
		dps, ok := metricsWithAttrs["test.semconv.published"]
		if !ok {
			t.Fatal("test.semconv.published metric not found")
		}

		found := false
		for _, dp := range dps {
			if hasAttr(dp.Attributes, "messaging.system", "rabbitmq") &&
				hasAttr(dp.Attributes, "messaging.destination.name", queueName) &&
				hasAttr(dp.Attributes, "messaging.operation.name", "send") &&
				hasAttr(dp.Attributes, "messaging.operation.type", "send") &&
				hasAttr(dp.Attributes, "server.address", "localhost") {
				found = true
				break
			}
		}
		if !found {
			t.Error("published metric with OTEL semantic convention attributes not found")
			for _, dp := range dps {
				t.Logf("Data point attributes: %v", dp.Attributes)
			}
		}
	})

	// Verify consumed metric has OTEL semantic convention attributes
	t.Run("consumed metric has semantic convention attributes", func(t *testing.T) {
		dps, ok := metricsWithAttrs["test.semconv.consumed"]
		if !ok {
			t.Fatal("test.semconv.consumed metric not found")
		}

		found := false
		for _, dp := range dps {
			if hasAttr(dp.Attributes, "messaging.system", "rabbitmq") &&
				hasAttr(dp.Attributes, "messaging.destination.name", queueName) &&
				hasAttr(dp.Attributes, "messaging.operation.name", "receive") &&
				hasAttr(dp.Attributes, "messaging.operation.type", "receive") &&
				hasAttr(dp.Attributes, "server.address", "localhost") {
				found = true
				break
			}
		}
		if !found {
			t.Error("consumed metric with OTEL semantic convention attributes not found")
			for _, dp := range dps {
				t.Logf("Data point attributes: %v", dp.Attributes)
			}
		}
	})

	// Verify consumed.accepted has settle operation type
	t.Run("consumed.accepted has settle operation attributes", func(t *testing.T) {
		dps, ok := metricsWithAttrs["test.semconv.consumed.accepted"]
		if !ok {
			t.Fatal("test.semconv.consumed.accepted metric not found")
		}

		found := false
		for _, dp := range dps {
			if hasAttr(dp.Attributes, "messaging.operation.name", "ack") &&
				hasAttr(dp.Attributes, "messaging.operation.type", "settle") {
				found = true
				break
			}
		}
		if !found {
			t.Error("consumed.accepted metric with settle operation attributes not found")
		}
	})
}

// extractMetricsWithDataPoints extracts metrics with their full data points for attribute checking.
func extractMetricsWithDataPoints(rm metricdata.ResourceMetrics) map[string][]metricdata.DataPoint[int64] {
	metrics := make(map[string][]metricdata.DataPoint[int64])
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				metrics[m.Name] = data.DataPoints
			case metricdata.Gauge[int64]:
				metrics[m.Name] = data.DataPoints
			}
		}
	}
	return metrics
}

// hasAttr checks if an attribute set contains a key-value pair.
func hasAttr(attrs attribute.Set, key, expectedValue string) bool {
	val, ok := attrs.Value(attribute.Key(key))
	if !ok {
		return false
	}
	return val.AsString() == expectedValue
}
