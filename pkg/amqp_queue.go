package pkg

import "context"

type AmqpQueueInfo struct {
}

func (a *AmqpQueueInfo) GetName() string {
	//TODO implement me
	panic("implement me")
}

type AmqpQueue struct {
	management *AmqpManagement
	name       string
}

func newAmqpQueue(management *AmqpManagement, queueName string) IQueueSpecification {
	return &AmqpQueue{management: management, name: queueName}
}

func (a *AmqpQueue) Declare(ctx context.Context) (error, IQueueInfo) {
	path := queuePath(a.name)
	kv := make(map[string]any)
	kv["durable"] = true
	kv["auto_delete"] = false
	_queueArguments := make(map[string]any)
	_queueArguments["x-queue-type"] = "quorum"
	kv["arguments"] = _queueArguments
	err := a.management.Request(ctx, "id", kv, path, commandPut, []int{200})
	if err != nil {
		return err, nil
	}
	return nil, &AmqpQueueInfo{}
}

func (a *AmqpQueue) Delete(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (a *AmqpQueue) Name(queueName string) IQueueSpecification {
	a.name = queueName
	return a
}

func (a *AmqpQueue) GetName() string {
	return a.name
}
