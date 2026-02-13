// RabbitMQ AMQP 1.0 Go Client: https://github.com/rabbitmq/rabbitmq-amqp-go-client
// RabbitMQ AMQP 1.0 documentation: https://www.rabbitmq.com/docs/amqp
//
// This example demonstrates how to configure OpenTelemetry metrics with a stdout exporter.
// Metrics are printed to stdout every 5 seconds, showing connection, publisher, consumer,
// and message counts.
//
// Prerequisites:
// - RabbitMQ 4.0+ running at localhost:5672
//
// Run with: go run main.go

package main

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
)

func main() {
	fmt.Println("=== RabbitMQ AMQP 1.0 Go Client - OTEL Metrics Example ===")
	fmt.Println()

	// 1. Create resource and stdout exporter for metrics
	// This will print metrics to stdout in JSON format
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("example-rabbitmq-amqp-go-client"),
			semconv.ServiceVersion("0.1.0"),
		),
	)
	if err != nil {
		fmt.Printf("Failed to create resource: %v\n", err)
		return
	}
	exporter, err := stdoutmetric.New()
	if err != nil {
		fmt.Printf("Failed to create stdout exporter: %v\n", err)
		return
	}

	// 2. Create MeterProvider with periodic reader
	// Metrics will be exported every 5 seconds
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(
			metric.NewPeriodicReader(exporter,
				metric.WithInterval(5*time.Second),
			),
		),
	)
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			fmt.Printf("Failed to shutdown meter provider: %v\n", err)
		}
	}()

	// 3. Create OTEL metrics collector from the library
	// Using default prefix "rabbitmq.amqp"
	collector, err := rmq.NewOTELMetricsCollector(meterProvider, rmq.DefaultMetricsPrefix)
	if err != nil {
		fmt.Printf("Failed to create OTEL metrics collector: %v\n", err)
		return
	}

	fmt.Println("OTEL MeterProvider configured with stdout exporter (5s interval)")
	fmt.Println()

	// 4. Create environment with metrics collector
	env := rmq.NewEnvironment("amqp://guest:guest@localhost:5672/", nil,
		rmq.WithMetricsCollector(collector))

	// 5. Open connection
	fmt.Println("Connecting to RabbitMQ...")
	connection, err := env.NewConnection(context.Background())
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	fmt.Println("Connected!")

	// 6. Declare a queue
	queueName := "otel-metrics-example-queue"
	management := connection.Management()
	_, err = management.DeclareQueue(context.Background(), &rmq.QuorumQueueSpecification{
		Name: queueName,
	})
	if err != nil {
		fmt.Printf("Failed to declare queue: %v\n", err)
		return
	}
	fmt.Printf("Queue '%s' declared\n", queueName)

	// 7. Create a publisher
	publisher, err := connection.NewPublisher(context.Background(), &rmq.QueueAddress{
		Queue: queueName,
	}, nil)
	if err != nil {
		fmt.Printf("Failed to create publisher: %v\n", err)
		return
	}
	fmt.Println("Publisher created")

	// 8. Create a consumer
	consumer, err := connection.NewConsumer(context.Background(), queueName, nil)
	if err != nil {
		fmt.Printf("Failed to create consumer: %v\n", err)
		return
	}
	fmt.Println("Consumer created")
	fmt.Println()

	// 9. Publish messages
	messageCount := 10
	fmt.Printf("Publishing %d messages...\n", messageCount)
	for i := 0; i < messageCount; i++ {
		msg := rmq.NewMessage([]byte(fmt.Sprintf("Message %d", i+1)))
		result, err := publisher.Publish(context.Background(), msg)
		if err != nil {
			fmt.Printf("Failed to publish message %d: %v\n", i+1, err)
			continue
		}
		switch result.Outcome.(type) {
		case *rmq.StateAccepted:
			// Message accepted
		default:
			fmt.Printf("Message %d not accepted: %v\n", i+1, result.Outcome)
		}
	}
	fmt.Printf("Published %d messages\n", messageCount)
	fmt.Println()

	// 10. Consume messages
	fmt.Printf("Consuming messages...\n")
	consumedCount := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for consumedCount < messageCount {
		delivery, err := consumer.Receive(ctx)
		if err != nil {
			fmt.Printf("Error receiving message: %v\n", err)
			break
		}
		err = delivery.Accept(context.Background())
		if err != nil {
			fmt.Printf("Error accepting message: %v\n", err)
			break
		}
		consumedCount++
	}
	fmt.Printf("Consumed and accepted %d messages\n", consumedCount)
	fmt.Println()

	// 11. Wait for metrics to be printed
	fmt.Println("Waiting 6 seconds for metrics to be exported to stdout...")
	fmt.Println("Look for metrics with names like:")
	fmt.Println("  - rabbitmq.amqp.connections")
	fmt.Println("  - rabbitmq.amqp.publishers")
	fmt.Println("  - rabbitmq.amqp.consumers")
	fmt.Println("  - rabbitmq.amqp.published")
	fmt.Println("  - rabbitmq.amqp.published.accepted")
	fmt.Println("  - rabbitmq.amqp.consumed")
	fmt.Println("  - rabbitmq.amqp.consumed.accepted")
	fmt.Println()
	time.Sleep(6 * time.Second)

	// 12. Cleanup
	fmt.Println()
	fmt.Println("Cleaning up...")

	if err := consumer.Close(context.Background()); err != nil {
		fmt.Printf("Failed to close consumer: %v\n", err)
	}

	if err := publisher.Close(context.Background()); err != nil {
		fmt.Printf("Failed to close publisher: %v\n", err)
	}

	if err := management.DeleteQueue(context.Background(), queueName); err != nil {
		fmt.Printf("Failed to delete queue: %v\n", err)
	}

	if err := connection.Close(context.Background()); err != nil {
		fmt.Printf("Failed to close connection: %v\n", err)
	}

	fmt.Println("Cleanup complete!")
	fmt.Println()

	// Wait for final metrics export showing closed resources
	fmt.Println("Waiting 6 more seconds for final metrics export (showing closed resources)...")
	time.Sleep(6 * time.Second)

	fmt.Println()
	fmt.Println("Example complete!")
}
