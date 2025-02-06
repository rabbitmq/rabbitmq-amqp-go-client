package rabbitmq_amqp

import (
	"context"
	"errors"
	"github.com/Azure/go-amqp"
)

type AMQPBindingInfo struct {
}

type AMQPBinding struct {
	sourceName      string
	destinationName string
	toQueue         bool
	bindingKey      string
	management      *AmqpManagement
}

func newAMQPBinding(management *AmqpManagement) *AMQPBinding {
	return &AMQPBinding{management: management}
}

func (b *AMQPBinding) BindingKey(bindingKey string) {
	b.bindingKey = bindingKey
}

func (b *AMQPBinding) SourceExchange(sourceName string) {
	if len(sourceName) > 0 {
		b.sourceName = sourceName
		b.toQueue = false
	}
}

func (b *AMQPBinding) Destination(name string, isQueue bool) {
	b.destinationName = name
	b.toQueue = isQueue
}

// Bind creates a binding between an exchange and a queue or exchange
// with the specified binding key.
// Returns the binding path that can be used to unbind the binding.
// Given a virtual host, the binding path is unique.
func (b *AMQPBinding) Bind(ctx context.Context) (string, error) {
	destination := "destination_queue"
	if !b.toQueue {
		destination = "destination_exchange"
	}

	if len(b.sourceName) == 0 || len(b.destinationName) == 0 {
		return "", errors.New("source and destination names are required")
	}

	path := bindingPath()
	kv := make(map[string]any)
	kv["binding_key"] = b.bindingKey
	kv["source"] = b.sourceName
	kv[destination] = b.destinationName
	kv["arguments"] = make(map[string]any)
	_, err := b.management.Request(ctx, kv, path, commandPost, []int{responseCode204})
	bindingPathWithExchangeQueueKey := bindingPathWithExchangeQueueKey(b.toQueue, b.sourceName, b.destinationName, b.bindingKey)
	return bindingPathWithExchangeQueueKey, err
}

// Unbind removes a binding between an exchange and a queue or exchange
// with the specified binding key.
// The bindingPath is the unique path that was returned when the binding was created.
func (b *AMQPBinding) Unbind(ctx context.Context, bindingPath string) error {
	_, err := b.management.Request(ctx, amqp.Null{}, bindingPath, commandDelete, []int{responseCode204})
	return err
}
