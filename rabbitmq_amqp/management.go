package rabbitmq_amqp

import (
	"context"
)

type IManagement interface {
	Open(ctx context.Context, connection IConnection) error
	Close(ctx context.Context) error
	Queue(queueName string) IQueueSpecification
	QueueInfo(ctx context.Context, queueName string) (IQueueInfo, error)
	Exchange(exchangeName string) IExchangeSpecification
	Binding() IBindingSpecification
	QueueClientName() IQueueSpecification
	GetStatus() int
	NotifyStatusChange(channel chan *StatusChanged)
	Request(ctx context.Context, body any, path string, method string,
		expectedResponseCodes []int) (map[string]any, error)
}
