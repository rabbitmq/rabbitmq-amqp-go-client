package rabbitmq_amqp

import (
	"context"
)

type AmqpQueueInfo struct {
	name         string
	isDurable    bool
	isAutoDelete bool
	isExclusive  bool
	leader       string
	replicas     []string
	arguments    map[string]any
	queueType    TQueueType
}

func (a *AmqpQueueInfo) GetLeader() string {
	return a.leader
}

func (a *AmqpQueueInfo) GetReplicas() []string {
	return a.replicas
}

func newAmqpQueueInfo(response map[string]any) IQueueInfo {
	return &AmqpQueueInfo{
		name:         response["name"].(string),
		isDurable:    response["durable"].(bool),
		isAutoDelete: response["auto_delete"].(bool),
		isExclusive:  response["exclusive"].(bool),
		queueType:    TQueueType(response["type"].(string)),
		leader:       response["leader"].(string),
		replicas:     response["replicas"].([]string),
		arguments:    response["arguments"].(map[string]any),
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

func (a *AmqpQueueInfo) Type() TQueueType {
	return a.queueType
}

func (a *AmqpQueueInfo) GetName() string {
	return a.name
}

func (a *AmqpQueueInfo) GetArguments() map[string]any {
	return a.arguments
}

type AmqpQueue struct {
	management     *AmqpManagement
	queueArguments map[string]any
	isExclusive    bool
	isAutoDelete   bool
	name           string
}

func (a *AmqpQueue) DeadLetterExchange(dlx string) IQueueSpecification {
	a.queueArguments["x-dead-letter-exchange"] = dlx
	return a
}

func (a *AmqpQueue) DeadLetterRoutingKey(dlrk string) IQueueSpecification {
	a.queueArguments["x-dead-letter-routing-key"] = dlrk
	return a
}

func (a *AmqpQueue) MaxLengthBytes(length int64) IQueueSpecification {
	a.queueArguments["max-length-bytes"] = length
	return a
}

func (a *AmqpQueue) QueueType(queueType QueueType) IQueueSpecification {
	a.queueArguments["x-queue-type"] = queueType.String()
	return a
}

func (a *AmqpQueue) GetQueueType() TQueueType {
	if a.queueArguments["x-queue-type"] == nil {
		return Classic
	}
	return TQueueType(a.queueArguments["x-queue-type"].(string))
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

func (a *AmqpQueue) validate() error {

	if a.queueArguments["max-length-bytes"] != nil {

		err := validatePositive("max length", a.queueArguments["max-length-bytes"].(int64))
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *AmqpQueue) Declare(ctx context.Context) (IQueueInfo, error) {

	if Quorum == a.GetQueueType() ||
		Stream == a.GetQueueType() {
		// mandatory arguments for quorum queues and streams
		a.Exclusive(false).AutoDelete(false)
	}
	if err := a.validate(); err != nil {
		return nil, err
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
