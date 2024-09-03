package pkg

import "context"

type IQueueSpecification interface {
	Name(queueName string) IQueueSpecification
	GetName() string
	Declare(ctx context.Context) (error, IQueueInfo)
	Delete(ctx context.Context) error
}

type IQueueInfo interface {
	GetName() string
}
