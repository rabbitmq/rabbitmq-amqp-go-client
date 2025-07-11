package rabbitmqamqp

import (
	"context"
	"errors"

	"github.com/Azure/go-amqp"
)

type AmqpQueueInfo struct {
	name          string
	isDurable     bool
	isAutoDelete  bool
	isExclusive   bool
	leader        string
	members       []string
	arguments     map[string]any
	queueType     TQueueType
	consumerCount uint32
	messageCount  uint64
}

func (a *AmqpQueueInfo) Leader() string {
	return a.leader
}

func (a *AmqpQueueInfo) Members() []string {
	return a.members
}

func newAmqpQueueInfo(response map[string]any) *AmqpQueueInfo {
	leader := ""
	if response["leader"] != nil {
		leader = response["leader"].(string)
	}

	return &AmqpQueueInfo{
		name:          response["name"].(string),
		isDurable:     response["durable"].(bool),
		isAutoDelete:  response["auto_delete"].(bool),
		isExclusive:   response["exclusive"].(bool),
		queueType:     TQueueType(response["type"].(string)),
		leader:        leader,
		members:       response["replicas"].([]string),
		arguments:     response["arguments"].(map[string]any),
		consumerCount: response["consumer_count"].(uint32),
		messageCount:  response["message_count"].(uint64),
	}
}

func (a *AmqpQueueInfo) IsDurable() bool {
	return a.isDurable
}

func (a *AmqpQueueInfo) IsAutoDelete() bool {
	return a.isAutoDelete
}

func (a *AmqpQueueInfo) IsExclusive() bool {
	return a.isExclusive
}

func (a *AmqpQueueInfo) Type() TQueueType {
	return a.queueType
}

func (a *AmqpQueueInfo) Name() string {
	return a.name
}

func (a *AmqpQueueInfo) Arguments() map[string]any {
	return a.arguments
}

func (a *AmqpQueueInfo) ConsumerCount() uint32 {
	return a.consumerCount
}

func (a *AmqpQueueInfo) MessageCount() uint64 {
	return a.messageCount
}

type AmqpQueue struct {
	management   *AmqpManagement
	arguments    map[string]any
	isExclusive  bool
	isAutoDelete bool
	name         string
}

func (a *AmqpQueue) Arguments(arguments map[string]any) {
	a.arguments = arguments
}

func (a *AmqpQueue) QueueType(queueType QueueType) {
	if len(queueType.String()) != 0 {
		a.arguments["x-queue-type"] = queueType.String()
	}
}

func (a *AmqpQueue) Exclusive(isExclusive bool) {
	a.isExclusive = isExclusive
}

func (a *AmqpQueue) IsExclusive() bool {
	return a.isExclusive
}

func (a *AmqpQueue) AutoDelete(isAutoDelete bool) {
	a.isAutoDelete = isAutoDelete
}

func (a *AmqpQueue) IsAutoDelete() bool {
	return a.isAutoDelete
}

func newAmqpQueue(management *AmqpManagement, queueName string) *AmqpQueue {
	return &AmqpQueue{management: management,
		name:      queueName,
		arguments: make(map[string]any)}
}

func (a *AmqpQueue) validate() error {
	if a.arguments["x-max-length-bytes"] != nil {
		err := validatePositive("max length", a.arguments["x-max-length-bytes"].(int64))
		if err != nil {
			return err
		}
	}

	if a.arguments["x-max-length"] != nil {
		err := validatePositive("max length", a.arguments["x-max-length"].(int64))
		if err != nil {
			return err
		}
	}
	if a.arguments["x-max-priority"] != nil {
		err := validatePositive("max priority", a.arguments["x-max-priority"].(int64))
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AmqpQueue) Declare(ctx context.Context) (*AmqpQueueInfo, error) {

	if err := a.validate(); err != nil {
		return nil, err
	}

	if a.name == "" {
		return nil, errors.New("queue name is required")
	}

	path, err := queueAddress(&a.name)
	if err != nil {
		return nil, err
	}
	kv := make(map[string]any)
	kv["durable"] = true
	kv["auto_delete"] = a.isAutoDelete
	kv["exclusive"] = a.isExclusive
	kv["arguments"] = a.arguments
	response, err := a.management.Request(ctx, kv, path, commandPut, []int{responseCode200, responseCode409})
	if err != nil {
		return nil, err
	}
	return newAmqpQueueInfo(response), nil
}

func (a *AmqpQueue) Delete(ctx context.Context) error {
	path, err := queueAddress(&a.name)
	if err != nil {
		return err
	}
	_, err = a.management.Request(ctx, amqp.Null{}, path, commandDelete, []int{responseCode200})
	return err
}

func (a *AmqpQueue) Purge(ctx context.Context) (int, error) {
	path, err := purgeQueueAddress(&a.name)
	if err != nil {
		return 0, err
	}

	response, err := a.management.Request(ctx, amqp.Null{}, path, commandDelete, []int{responseCode200})
	return int(response["message_count"].(uint64)), err
}

func (a *AmqpQueue) Name(queueName string) {
	a.name = queueName
}
