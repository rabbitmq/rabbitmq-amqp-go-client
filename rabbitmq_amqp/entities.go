package rabbitmq_amqp

import "context"

const (
	Quorum  = "quorum"
	Classic = "classic"
	Stream  = "stream"
)

type IEntityInfoSpecification[T any] interface {
	Declare(ctx context.Context) (T, error)
	Delete(ctx context.Context) error
}

type IQueueSpecification interface {
	GetName() string
	Exclusive(isExclusive bool) IQueueSpecification
	IsExclusive() bool
	AutoDelete(isAutoDelete bool) IQueueSpecification
	IsAutoDelete() bool
	IEntityInfoSpecification[IQueueInfo]
	QueueType(queueType string) IQueueSpecification
	GetQueueType() string
}

type IQueueInfo interface {
	GetName() string
	IsDurable() bool
	IsAutoDelete() bool
	Exclusive() bool
	Type() string
}
