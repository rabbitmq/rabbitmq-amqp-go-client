package rabbitmq_amqp

import (
	"context"
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

func (b *AMQPBinding) DestinationExchange(destinationName string) {
	if len(destinationName) > 0 {
		b.destinationName = destinationName
		b.toQueue = false
	}
}

func (b *AMQPBinding) DestinationQueue(queueName string) {
	if len(queueName) > 0 {
		b.destinationName = queueName
		b.toQueue = true
	}
}

func (b *AMQPBinding) Bind(ctx context.Context) (string, error) {
	path := bindingPath()
	kv := make(map[string]any)
	kv["binding_key"] = b.bindingKey
	kv["source"] = b.sourceName
	kv["destination_queue"] = b.destinationName
	kv["arguments"] = make(map[string]any)
	_, err := b.management.Request(ctx, kv, path, commandPost, []int{responseCode204})
	bindingPathWithExchangeQueueKey := bindingPathWithExchangeQueueKey(b.toQueue, b.sourceName, b.destinationName, b.bindingKey)
	return bindingPathWithExchangeQueueKey, err
}

func (b *AMQPBinding) Unbind(ctx context.Context, bindingPath string) error {
	_, err := b.management.Request(ctx, amqp.Null{}, bindingPath, commandDelete, []int{responseCode204})
	return err
}
