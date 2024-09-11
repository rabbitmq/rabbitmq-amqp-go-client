package rabbitmq_amqp

import "context"

type AMQPBindingInfo struct {
}

type AMQPBinding struct {
	sourceExchangeName string
	destinationQueue   string
	bindingKey         string
	management         *AmqpManagement
}

func newAMQPBinding(management *AmqpManagement) *AMQPBinding {
	return &AMQPBinding{management: management}
}

func (b *AMQPBinding) Key(bindingKey string) IBindingSpecification {
	b.bindingKey = bindingKey
	return b
}

func (b *AMQPBinding) SourceExchange(exchangeName string) IBindingSpecification {
	b.sourceExchangeName = exchangeName
	return b
}

func (b *AMQPBinding) DestinationQueue(queueName string) IBindingSpecification {
	b.destinationQueue = queueName
	return b
}

func (b *AMQPBinding) Bind(ctx context.Context) error {

	path := bindingPath()
	kv := make(map[string]any)
	kv["binding_key"] = b.bindingKey
	kv["source"] = b.sourceExchangeName
	kv["destination_queue"] = b.destinationQueue
	kv["arguments"] = make(map[string]any)
	_, err := b.management.Request(ctx, kv, path, commandPost, []int{responseCode204})
	return err

}

func (b *AMQPBinding) Unbind(ctx context.Context) error {
	bindingPathWithExchangeQueueKey := bindingPathWithExchangeQueueKey(b.sourceExchangeName, b.destinationQueue, b.bindingKey)
	_, err := b.management.Request(ctx, nil, bindingPathWithExchangeQueueKey, commandDelete, []int{responseCode204})
	return err
}
