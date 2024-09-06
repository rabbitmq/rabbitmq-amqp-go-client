package rabbitmq_amqp

import (
	"context"
	"strings"
)

type AmqpQueueInfo struct {
	name         string
	isDurable    bool
	isAutoDelete bool
	isExclusive  bool
	queueType    string
}

func newAmqpQueueInfo(response map[string]any) IQueueInfo {
	return &AmqpQueueInfo{
		name:         response["name"].(string),
		isDurable:    response["durable"].(bool),
		isAutoDelete: response["auto_delete"].(bool),
		isExclusive:  response["exclusive"].(bool),
		queueType:    response["type"].(string),
	}
}

func (a *AmqpQueueInfo) IsDurable() bool {
	return a.isDurable
}

func (a *AmqpQueueInfo) IsAutoDelete() bool {
	return a.isAutoDelete
}

func (a *AmqpQueueInfo) Exclusive() bool {
	return a.isExclusive
}

func (a *AmqpQueueInfo) Type() string {
	return a.queueType
}

func (a *AmqpQueueInfo) GetName() string {
	return a.name
}

type AmqpQueue struct {
	management     *AmqpManagement
	queueArguments map[string]any
	isExclusive    bool
	isAutoDelete   bool
	name           string
}

func (a *AmqpQueue) QueueType(queueType string) IQueueSpecification {
	a.queueArguments["x-queue-type"] = queueType
	return a
}

func (a *AmqpQueue) GetQueueType() string {
	if a.queueArguments["x-queue-type"] == nil {
		return Classic
	}

	return a.queueArguments["x-queue-type"].(string)
}

func (a *AmqpQueue) Exclusive(isExclusive bool) IQueueSpecification {
	a.isExclusive = isExclusive
	return a
}

func (a *AmqpQueue) IsExclusive() bool {
	return a.isExclusive
}

func (a *AmqpQueue) AutoDelete(isAutoDelete bool) IQueueSpecification {
	a.isAutoDelete = isAutoDelete
	return a
}

func (a *AmqpQueue) IsAutoDelete() bool {
	return a.isAutoDelete
}

func newAmqpQueue(management *AmqpManagement, queueName string) IQueueSpecification {
	return &AmqpQueue{management: management,
		name:           queueName,
		queueArguments: make(map[string]any)}
}

func (a *AmqpQueue) Declare(ctx context.Context) (IQueueInfo, error) {

	if Quorum == strings.ToLower(a.GetQueueType()) ||
		// mandatory arguments for quorum queues and streams
		Stream == strings.ToLower(a.GetQueueType()) {
		a.Exclusive(false).AutoDelete(false)
	}

	path := queuePath(a.name)
	kv := make(map[string]any)
	kv["durable"] = true
	kv["auto_delete"] = a.isAutoDelete
	kv["exclusive"] = a.isExclusive
	kv["arguments"] = a.queueArguments
	response, err := a.management.Request(ctx, kv, path, commandPut, []int{200})
	if err != nil {
		return nil, err
	}
	return newAmqpQueueInfo(response), nil
}

func (a *AmqpQueue) Delete(ctx context.Context) error {
	path := queuePath(a.name)
	_, err := a.management.Request(ctx, nil, path, commandDelete, []int{200})
	if err != nil {
		return err
	}
	return nil

}

func (a *AmqpQueue) Name(queueName string) IQueueSpecification {
	a.name = queueName
	return a
}

func (a *AmqpQueue) GetName() string {
	return a.name
}
