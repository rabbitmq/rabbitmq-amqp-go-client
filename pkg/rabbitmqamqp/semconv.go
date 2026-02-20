package rabbitmqamqp

import "go.opentelemetry.io/otel/attribute"

// OTEL Semantic Convention attribute keys for RabbitMQ messaging.
// See: https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/
const (
	// Required attributes
	MessagingSystemKey          = attribute.Key("messaging.system")
	MessagingDestinationNameKey = attribute.Key("messaging.destination.name")
	MessagingOperationNameKey   = attribute.Key("messaging.operation.name")
	MessagingOperationTypeKey   = attribute.Key("messaging.operation.type")

	// Server attributes
	ServerAddressKey = attribute.Key("server.address")
	ServerPortKey    = attribute.Key("server.port")

	// RabbitMQ-specific attributes
	MessagingRabbitMQRoutingKeyKey = attribute.Key("messaging.rabbitmq.destination.routing_key")

	// Message attributes
	MessagingMessageIDKey = attribute.Key("messaging.message.id")
)

// Constant values for messaging.system
const (
	MessagingSystemRabbitMQ = "rabbitmq"
)

// Operation types per OTEL semantic conventions
const (
	OperationTypeSend    = "send"
	OperationTypeReceive = "receive"
	OperationTypeSettle  = "settle"
)

// Operation names per OTEL semantic conventions
const (
	OperationNameSend    = "send"
	OperationNameReceive = "receive"
	OperationNameAck     = "ack"
	OperationNameNack    = "nack"
	OperationNameRequeue = "requeue"
)

// buildDestinationName constructs the messaging.destination.name attribute value
// according to OTEL semantic conventions for RabbitMQ.
//
// For producers: "{exchange}:{routing_key}" or just "{exchange}" or "{queue}"
// For consumers: "{queue}" or "{exchange}:{routing_key}:{queue}"
func buildDestinationName(exchange, routingKey, queue string) string {
	if queue != "" && exchange == "" {
		return queue
	}
	if routingKey != "" {
		return exchange + ":" + routingKey
	}
	if exchange != "" {
		return exchange
	}
	return queue
}
