package pkg

type IManagement interface {
	Open() error
	Close() error
	Queue(queueName string) IQueueSpecification
}
