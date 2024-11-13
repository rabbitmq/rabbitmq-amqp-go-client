package rabbitmq_amqp

import (
	"context"
)

type IManagement interface {
	Open(ctx context.Context, connection IConnection) error
	Close(ctx context.Context) error
	DeclareQueue(ctx context.Context, specification *QueueSpecification) (IQueueInfo, error)
	DeleteQueue(ctx context.Context, name string) error
	DeclareExchange(ctx context.Context, exchangeSpecification *ExchangeSpecification) (IExchangeInfo, error)
	DeleteExchange(ctx context.Context, name string) error

	Bind(ctx context.Context, bindingSpecification *BindingSpecification) (string, error)
	Unbind(ctx context.Context, bindingPath string) error

	PurgeQueue(ctx context.Context, queueName string) (int, error)

	QueueInfo(ctx context.Context, queueName string) (IQueueInfo, error)
	Status() int
	NotifyStatusChange(channel chan *StatusChanged)
	Request(ctx context.Context, body any, path string, method string,
		expectedResponseCodes []int) (map[string]any, error)
}
