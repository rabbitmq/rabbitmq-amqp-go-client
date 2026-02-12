module github.com/rabbitmq/rabbitmq-amqp-go-client/tests/otelmetrics

go 1.24.0

replace github.com/rabbitmq/rabbitmq-amqp-go-client => ../..

require (
	github.com/rabbitmq/rabbitmq-amqp-go-client v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.40.0
	go.opentelemetry.io/otel/sdk/metric v1.40.0
)

require (
	github.com/Azure/go-amqp v1.5.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/sdk v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
)
