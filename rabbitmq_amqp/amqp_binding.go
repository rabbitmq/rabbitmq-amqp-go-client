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

func (b *AMQPBinding) Key(bindingKey string) IBindingSpecification {
	b.bindingKey = bindingKey
	return b
}

func (b *AMQPBinding) SourceExchange(exchangeSpec IExchangeSpecification) IBindingSpecification {
	b.sourceName = exchangeSpec.GetName()
	b.toQueue = false
	return b
}

func (b *AMQPBinding) SourceExchangeName(exchangeName string) IBindingSpecification {
	b.sourceName = exchangeName
	b.toQueue = false
	return b
}

func (b *AMQPBinding) DestinationExchange(exchangeSpec IExchangeInfo) IBindingSpecification {
	b.destinationName = exchangeSpec.GetName()
	b.toQueue = false
	return b
}

func (b *AMQPBinding) DestinationExchangeName(exchangeName string) IBindingSpecification {
	b.destinationName = exchangeName
	b.toQueue = false
	return b
}

func (b *AMQPBinding) DestinationQueue(queueSpec IQueueSpecification) IBindingSpecification {
	b.destinationName = queueSpec.GetName()
	b.toQueue = true
	return b
}

func (b *AMQPBinding) DestinationQueueName(queueName string) IBindingSpecification {
	b.destinationName = queueName
	b.toQueue = true
	return b
}

func (b *AMQPBinding) Bind(ctx context.Context) error {
	path := bindingPath()
	kv := make(map[string]any)
	kv["binding_key"] = b.bindingKey
	kv["source"] = b.sourceName
	kv["destination_queue"] = b.destinationName
	kv["arguments"] = make(map[string]any)
	_, err := b.management.Request(ctx, kv, path, commandPost, []int{responseCode204})
	return err
}

func (b *AMQPBinding) Unbind(ctx context.Context) error {
	bindingPathWithExchangeQueueKey := bindingPathWithExchangeQueueKey(b.toQueue, b.sourceName, b.destinationName, b.bindingKey)
	_, err := b.management.Request(ctx, amqp.Null{}, bindingPathWithExchangeQueueKey, commandDelete, []int{responseCode204})
	return err
}
