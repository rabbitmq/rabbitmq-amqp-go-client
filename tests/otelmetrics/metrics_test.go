package otelmetrics_test

import (
	"context"
	"sync"
	"testing"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestMetricsCollectorInterface verifies that both NoOp and OTEL collectors implement the interface.
func TestMetricsCollectorInterface(t *testing.T) {
	// Verify NoOpMetricsCollector implements the interface
	var _ rmq.MetricsCollector = &rmq.NoOpMetricsCollector{}
	var _ rmq.MetricsCollector = rmq.DefaultMetricsCollector()

	// Verify OTELMetricsCollector implements the interface
	meterProvider := metric.NewMeterProvider()
	defer meterProvider.Shutdown(context.Background())

	otelCollector, err := rmq.NewOTELMetricsCollector(meterProvider, "")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	var _ rmq.MetricsCollector = otelCollector
}

// TestNoOpMetricsCollector verifies that no-op collector methods can be called without error.
func TestNoOpMetricsCollector(t *testing.T) {
	collector := &rmq.NoOpMetricsCollector{}

	// Context structs for publish/consume methods
	publishCtx := rmq.PublishContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
		RoutingKey:      "test-key",
		MessageID:       "msg-123",
	}
	consumeCtx := rmq.ConsumeContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
		MessageID:       "msg-123",
	}

	// All methods should complete without panicking
	collector.OpenConnection()
	collector.CloseConnection()
	collector.OpenPublisher()
	collector.ClosePublisher()
	collector.OpenConsumer()
	collector.CloseConsumer()
	collector.Publish(publishCtx)
	collector.PublishDisposition(rmq.PublishAccepted, publishCtx)
	collector.PublishDisposition(rmq.PublishRejected, publishCtx)
	collector.PublishDisposition(rmq.PublishReleased, publishCtx)
	collector.Consume(consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeAccepted, consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeDiscarded, consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeRequeued, consumeCtx)
	collector.WrittenBytes(1024)
	collector.ReadBytes(2048)
}

// TestOTELMetricsCollectorCreation verifies that OTEL collector can be created with various options.
func TestOTELMetricsCollectorCreation(t *testing.T) {
	meterProvider := metric.NewMeterProvider()
	defer meterProvider.Shutdown(context.Background())

	// Test with default prefix
	collector1, err := rmq.NewOTELMetricsCollector(meterProvider, "")
	if err != nil {
		t.Fatalf("failed to create collector with default prefix: %v", err)
	}
	if collector1 == nil {
		t.Fatal("collector should not be nil")
	}

	// Test with custom prefix
	collector2, err := rmq.NewOTELMetricsCollector(meterProvider, "custom.prefix")
	if err != nil {
		t.Fatalf("failed to create collector with custom prefix: %v", err)
	}
	if collector2 == nil {
		t.Fatal("collector should not be nil")
	}
}

// TestOTELMetricsCollectorRecordsMetrics verifies that OTEL collector records metrics correctly.
func TestOTELMetricsCollectorRecordsMetrics(t *testing.T) {
	// Create a manual reader to capture metrics
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.rabbitmq")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	// Context structs for publish/consume methods
	publishCtx := rmq.PublishContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}
	consumeCtx := rmq.ConsumeContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}

	// Simulate connection lifecycle
	collector.OpenConnection()
	collector.OpenConnection()  // Open 2 connections
	collector.CloseConnection() // Close 1

	// Simulate publisher lifecycle
	collector.OpenPublisher()
	collector.Publish(publishCtx)
	collector.Publish(publishCtx)
	collector.Publish(publishCtx)
	collector.PublishDisposition(rmq.PublishAccepted, publishCtx)
	collector.PublishDisposition(rmq.PublishAccepted, publishCtx)
	collector.PublishDisposition(rmq.PublishRejected, publishCtx)
	collector.ClosePublisher()

	// Simulate consumer lifecycle
	collector.OpenConsumer()
	collector.Consume(consumeCtx)
	collector.Consume(consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeAccepted, consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeRequeued, consumeCtx)
	collector.CloseConsumer()

	// Simulate byte tracking
	collector.WrittenBytes(1000)
	collector.WrittenBytes(500)
	collector.ReadBytes(2000)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	// Verify we have metrics
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected scope metrics to be recorded")
	}

	// Create a map of metric names to their values for easier verification
	metrics := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					metrics[m.Name] = dp.Value
				}
			}
		}
	}

	// Verify gauge metrics (connections should be 1: 2 opened - 1 closed)
	if val, ok := metrics["test.rabbitmq.connections"]; !ok || val != 1 {
		t.Errorf("expected connections gauge to be 1, got %d (found: %v)", val, ok)
	}

	// Verify publishers gauge (should be 0: 1 opened - 1 closed)
	if val, ok := metrics["test.rabbitmq.publishers"]; !ok || val != 0 {
		t.Errorf("expected publishers gauge to be 0, got %d (found: %v)", val, ok)
	}

	// Verify consumers gauge (should be 0: 1 opened - 1 closed)
	if val, ok := metrics["test.rabbitmq.consumers"]; !ok || val != 0 {
		t.Errorf("expected consumers gauge to be 0, got %d (found: %v)", val, ok)
	}

	// Verify counter metrics
	if val, ok := metrics["test.rabbitmq.published"]; !ok || val != 3 {
		t.Errorf("expected published counter to be 3, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.published.accepted"]; !ok || val != 2 {
		t.Errorf("expected published.accepted counter to be 2, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.published.rejected"]; !ok || val != 1 {
		t.Errorf("expected published.rejected counter to be 1, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.consumed"]; !ok || val != 2 {
		t.Errorf("expected consumed counter to be 2, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.consumed.accepted"]; !ok || val != 1 {
		t.Errorf("expected consumed.accepted counter to be 1, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.consumed.requeued"]; !ok || val != 1 {
		t.Errorf("expected consumed.requeued counter to be 1, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.written_bytes"]; !ok || val != 1500 {
		t.Errorf("expected written_bytes counter to be 1500, got %d (found: %v)", val, ok)
	}

	if val, ok := metrics["test.rabbitmq.read_bytes"]; !ok || val != 2000 {
		t.Errorf("expected read_bytes counter to be 2000, got %d (found: %v)", val, ok)
	}
}

// TestEnvironmentWithMetrics verifies that environment can be created with a metrics collector.
func TestEnvironmentWithMetrics(t *testing.T) {
	meterProvider := metric.NewMeterProvider()
	defer meterProvider.Shutdown(context.Background())

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	// Test NewEnvironment with metrics collector
	env := rmq.NewEnvironment("amqp://localhost", nil,
		rmq.WithMetricsCollector(collector))
	if env == nil {
		t.Fatal("environment should not be nil")
	}

	// Test NewClusterEnvironment with metrics collector
	endpoints := []rmq.Endpoint{
		{Address: "amqp://localhost:5672", Options: nil},
	}
	env2 := rmq.NewClusterEnvironment(endpoints,
		rmq.WithMetricsCollector(collector))
	if env2 == nil {
		t.Fatal("cluster environment should not be nil")
	}

	// Test NewClusterEnvironment with strategy and metrics collector
	env3 := rmq.NewClusterEnvironment(endpoints,
		rmq.WithStrategy(rmq.StrategySequential),
		rmq.WithMetricsCollector(collector))
	if env3 == nil {
		t.Fatal("cluster environment with strategy should not be nil")
	}

	// Test with no options (should use defaults)
	env4 := rmq.NewEnvironment("amqp://localhost", nil)
	if env4 == nil {
		t.Fatal("environment with no options should not be nil")
	}
}

// TestPublishDispositionEnums verifies the publish disposition enum values.
func TestPublishDispositionEnums(t *testing.T) {
	// Verify enum values are distinct
	if rmq.PublishAccepted == rmq.PublishRejected {
		t.Error("PublishAccepted should not equal PublishRejected")
	}
	if rmq.PublishAccepted == rmq.PublishReleased {
		t.Error("PublishAccepted should not equal PublishReleased")
	}
	if rmq.PublishRejected == rmq.PublishReleased {
		t.Error("PublishRejected should not equal PublishReleased")
	}
}

// TestConsumeDispositionEnums verifies the consume disposition enum values.
func TestConsumeDispositionEnums(t *testing.T) {
	// Verify enum values are distinct
	if rmq.ConsumeAccepted == rmq.ConsumeDiscarded {
		t.Error("ConsumeAccepted should not equal ConsumeDiscarded")
	}
	if rmq.ConsumeAccepted == rmq.ConsumeRequeued {
		t.Error("ConsumeAccepted should not equal ConsumeRequeued")
	}
	if rmq.ConsumeDiscarded == rmq.ConsumeRequeued {
		t.Error("ConsumeDiscarded should not equal ConsumeRequeued")
	}
}

// TestDefaultMetricsPrefix verifies the default metrics prefix constant.
func TestDefaultMetricsPrefix(t *testing.T) {
	if rmq.DefaultMetricsPrefix != "rabbitmq.amqp" {
		t.Errorf("expected default prefix to be 'rabbitmq.amqp', got '%s'", rmq.DefaultMetricsPrefix)
	}
}

// TestMetricsCollectorThreadSafety verifies that concurrent calls to metrics collector don't panic.
func TestMetricsCollectorThreadSafety(t *testing.T) {
	meterProvider := metric.NewMeterProvider()
	defer meterProvider.Shutdown(context.Background())

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	// Context structs for publish/consume methods
	publishCtx := rmq.PublishContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}
	consumeCtx := rmq.ConsumeContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}

	// Run concurrent operations
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				collector.OpenConnection()
				collector.OpenPublisher()
				collector.OpenConsumer()
				collector.Publish(publishCtx)
				collector.PublishDisposition(rmq.PublishAccepted, publishCtx)
				collector.Consume(consumeCtx)
				collector.ConsumeDisposition(rmq.ConsumeAccepted, consumeCtx)
				collector.WrittenBytes(100)
				collector.ReadBytes(100)
				collector.CloseConsumer()
				collector.ClosePublisher()
				collector.CloseConnection()
			}
		}()
	}

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for goroutines")
	}
}

// TestOTELMetricsCollectorWithContextMethods verifies that methods record metrics with OTEL semantic convention attributes.
func TestOTELMetricsCollectorWithContextMethods(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.ctx")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	// Test Publish with context
	publishCtx := rmq.PublishContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-exchange:test-key",
		RoutingKey:      "test-key",
		MessageID:       "msg-123",
	}
	collector.Publish(publishCtx)
	collector.Publish(publishCtx)
	collector.PublishDisposition(rmq.PublishAccepted, publishCtx)
	collector.PublishDisposition(rmq.PublishRejected, publishCtx)

	// Test Consume with context
	consumeCtx := rmq.ConsumeContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
		MessageID:       "msg-456",
	}
	collector.Consume(consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeAccepted, consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeDiscarded, consumeCtx)
	collector.ConsumeDisposition(rmq.ConsumeRequeued, consumeCtx)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	// Verify we have metrics
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected scope metrics to be recorded")
	}

	// Create a map of metric names to their data points for verification
	metricsWithAttrs := make(map[string][]metricdata.DataPoint[int64])
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				metricsWithAttrs[m.Name] = data.DataPoints
			}
		}
	}

	// Verify published metric has attributes
	t.Run("published with context has attributes", func(t *testing.T) {
		dps, ok := metricsWithAttrs["test.ctx.published"]
		if !ok {
			t.Fatal("test.ctx.published metric not found")
		}

		// Find the data point with our attributes
		found := false
		for _, dp := range dps {
			attrs := dp.Attributes
			if hasAttribute(attrs, "messaging.system", "rabbitmq") &&
				hasAttribute(attrs, "messaging.destination.name", "test-exchange:test-key") &&
				hasAttribute(attrs, "messaging.operation.name", "send") &&
				hasAttribute(attrs, "messaging.operation.type", "send") {
				found = true
				if dp.Value != 2 {
					t.Errorf("expected 2 published messages with context, got %d", dp.Value)
				}
				break
			}
		}
		if !found {
			t.Error("published metric with OTEL semantic convention attributes not found")
		}
	})

	// Verify consumed metric has attributes
	t.Run("consumed with context has attributes", func(t *testing.T) {
		dps, ok := metricsWithAttrs["test.ctx.consumed"]
		if !ok {
			t.Fatal("test.ctx.consumed metric not found")
		}

		found := false
		for _, dp := range dps {
			attrs := dp.Attributes
			if hasAttribute(attrs, "messaging.system", "rabbitmq") &&
				hasAttribute(attrs, "messaging.destination.name", "test-queue") &&
				hasAttribute(attrs, "messaging.operation.name", "receive") &&
				hasAttribute(attrs, "messaging.operation.type", "receive") {
				found = true
				if dp.Value != 1 {
					t.Errorf("expected 1 consumed message with context, got %d", dp.Value)
				}
				break
			}
		}
		if !found {
			t.Error("consumed metric with OTEL semantic convention attributes not found")
		}
	})

	// Verify consume disposition metrics have attributes
	t.Run("consume dispositions with context have attributes", func(t *testing.T) {
		// Check consumed.accepted
		dps, ok := metricsWithAttrs["test.ctx.consumed.accepted"]
		if !ok {
			t.Error("test.ctx.consumed.accepted metric not found")
		} else {
			found := false
			for _, dp := range dps {
				if hasAttribute(dp.Attributes, "messaging.operation.name", "ack") {
					found = true
					break
				}
			}
			if !found {
				t.Error("consumed.accepted with ack operation name not found")
			}
		}

		// Check consumed.discarded
		dps, ok = metricsWithAttrs["test.ctx.consumed.discarded"]
		if !ok {
			t.Error("test.ctx.consumed.discarded metric not found")
		} else {
			found := false
			for _, dp := range dps {
				if hasAttribute(dp.Attributes, "messaging.operation.name", "nack") {
					found = true
					break
				}
			}
			if !found {
				t.Error("consumed.discarded with nack operation name not found")
			}
		}

		// Check consumed.requeued
		dps, ok = metricsWithAttrs["test.ctx.consumed.requeued"]
		if !ok {
			t.Error("test.ctx.consumed.requeued metric not found")
		} else {
			found := false
			for _, dp := range dps {
				if hasAttribute(dp.Attributes, "messaging.operation.name", "requeue") {
					found = true
					break
				}
			}
			if !found {
				t.Error("consumed.requeued with requeue operation name not found")
			}
		}
	})
}

// hasAttribute checks if an attribute set contains a key-value pair.
func hasAttribute(attrs attribute.Set, key, expectedValue string) bool {
	val, ok := attrs.Value(attribute.Key(key))
	if !ok {
		return false
	}
	return val.AsString() == expectedValue
}

// TestDoubleCountingBug proves that calling both Publish() and PublishWithContext()
// (or similar pairs) results in double-counting metrics.
// This test should FAIL with the current buggy implementation, proving the bug exists.
func TestDoubleCountingBug(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer meterProvider.Shutdown(context.Background())

	collector, err := rmq.NewOTELMetricsCollector(meterProvider, "test.double")
	if err != nil {
		t.Fatalf("failed to create OTEL metrics collector: %v", err)
	}

	// After the fix, each metric method should only be called ONCE per operation.
	// The *WithContext methods no longer exist - context is now passed directly.
	// This test verifies that the fix is correct: 1 call = 1 count.

	publishCtx := rmq.PublishContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}

	// Simulate publishing ONE message (the fixed pattern - single call)
	collector.Publish(publishCtx)

	// Simulate consuming ONE message (the fixed pattern - single call)
	consumeCtx := rmq.ConsumeContext{
		ServerAddress:   "localhost",
		ServerPort:      5672,
		DestinationName: "test-queue",
	}
	collector.Consume(consumeCtx)

	// Simulate ONE disposition (the fixed pattern - single call)
	collector.ConsumeDisposition(rmq.ConsumeAccepted, consumeCtx)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	// Sum all data points for each metric (they may have different attribute sets)
	metricTotals := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					metricTotals[m.Name] += dp.Value
				}
			}
		}
	}

	// These assertions verify the CORRECT behavior (1 message = 1 count)
	// With the current buggy code, these will FAIL because counts will be 2

	t.Run("published should count once per message", func(t *testing.T) {
		if total := metricTotals["test.double.published"]; total != 1 {
			t.Errorf("BUG DETECTED: published metric should be 1 for one message, got %d (double-counting!)", total)
		}
	})

	t.Run("consumed should count once per message", func(t *testing.T) {
		if total := metricTotals["test.double.consumed"]; total != 1 {
			t.Errorf("BUG DETECTED: consumed metric should be 1 for one message, got %d (double-counting!)", total)
		}
	})

	t.Run("consumed.accepted should count once per disposition", func(t *testing.T) {
		if total := metricTotals["test.double.consumed.accepted"]; total != 1 {
			t.Errorf("BUG DETECTED: consumed.accepted metric should be 1 for one disposition, got %d (double-counting!)", total)
		}
	})
}
