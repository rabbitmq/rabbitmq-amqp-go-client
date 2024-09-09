package rabbitmq_amqp

import (
	"context"
)

type TQueueType string

const (
	Quorum  TQueueType = "quorum"
	Classic TQueueType = "classic"
	Stream  TQueueType = "stream"
)

type QueueType struct {
	Type TQueueType
}

func (e QueueType) String() string {
	return string(e.Type)
}

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
	QueueType(queueType QueueType) IQueueSpecification
	GetQueueType() TQueueType

	MaxLengthBytes(length int64) IQueueSpecification
	DeadLetterExchange(dlx string) IQueueSpecification
	DeadLetterRoutingKey(dlrk string) IQueueSpecification
}

type IQueueInfo interface {
	GetName() string
	IsDurable() bool
	IsAutoDelete() bool
	Exclusive() bool
	Type() TQueueType
	GetLeader() string
	GetReplicas() []string
	GetArguments() map[string]any
}
