package rabbitmq_amqp

import "context"

type IQueueSpecification interface {
	GetName() string
	Declare(ctx context.Context) (error, IQueueInfo)
	Delete(ctx context.Context) error
}

type IQueueInfo interface {
	GetName() string
}
