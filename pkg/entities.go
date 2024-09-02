package pkg

type IQueueSpecification interface {
	Name(queueName string) IQueueSpecification
	GetName() string
}
