package rabbitmqamqp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

var ErrPreconditionFailed = errors.New("precondition Failed")
var ErrDoesNotExist = errors.New("does not exist")

/*
AmqpManagement is the interface to the RabbitMQ /management endpoint
The management interface is used to declare/delete exchanges, queues, and bindings
*/
type AmqpManagement struct {
	session   *amqp.Session
	sender    *amqp.Sender
	receiver  *amqp.Receiver
	lifeCycle *LifeCycle
	// Topology Recovery Options and Records are references from AmqpConnection
	// to manage the topology recovery records.
	topologyRecoveryOptions TopologyRecoveryOptions
	topologyRecoveryRecords *topologyRecoveryRecords
	// isRecovering indicates whether we're currently recovering topology
	// to prevent duplicate recovery records. Protected by atomic operations
	// since recovery happens in a separate goroutine while public API methods
	// can be called from user goroutines.
	isRecovering atomic.Bool
}

func newAmqpManagement(topologyRecovery TopologyRecoveryOptions) *AmqpManagement {
	return &AmqpManagement{
		lifeCycle:               NewLifeCycle(),
		topologyRecoveryOptions: topologyRecovery,
	}
}

func (a *AmqpManagement) ensureReceiverLink(ctx context.Context) error {
	opts := createReceiverLinkOptions(managementNodeAddress, &managementOptions{}, AtMostOnce)
	receiver, err := a.session.NewReceiver(ctx, managementNodeAddress, opts)
	if err != nil {
		return err
	}
	a.receiver = receiver
	return nil
}

func (a *AmqpManagement) ensureSenderLink(ctx context.Context) error {
	sender, err := a.session.NewSender(ctx, managementNodeAddress,
		createSenderLinkOptions(managementNodeAddress, linkPairName, AtMostOnce))
	if err != nil {
		return err
	}

	a.sender = sender
	return nil
}

func (a *AmqpManagement) Open(ctx context.Context, connection *AmqpConnection) error {
	session, err := connection.azureConnection.NewSession(ctx, nil)
	if err != nil {
		return err
	}

	a.session = session
	err = a.ensureSenderLink(ctx)
	if err != nil {
		return err
	}

	err = a.ensureReceiverLink(ctx)
	if err != nil {
		return err
	}

	// TODO
	// Even 10ms is enough to allow the links to establish,
	// which tells me it allows the golang runtime to process
	// some channels or I/O or something elsewhere
	time.Sleep(time.Millisecond * 10)

	a.lifeCycle.SetState(&StateOpen{})
	return ctx.Err()
}

func (a *AmqpManagement) Close(ctx context.Context) error {
	_ = a.sender.Close(ctx)
	_ = a.receiver.Close(ctx)
	err := a.session.Close(ctx)
	a.lifeCycle.SetState(&StateClosed{})
	return err
}

/*
Request sends a request to the /management endpoint.
It is a generic method that can be used to send any request to the management endpoint.
In most of the cases you don't need to use this method directly, instead use the standard methods
*/
func (a *AmqpManagement) Request(ctx context.Context, body any, path string, method string,
	expectedResponseCodes []int) (map[string]any, error) {
	return a.request(ctx, uuid.New().String(), body, path, method, expectedResponseCodes)
}

func (a *AmqpManagement) validateResponseCode(responseCode int, expectedResponseCodes []int) error {
	if responseCode == responseCode409 {
		return ErrPreconditionFailed
	}

	for _, code := range expectedResponseCodes {
		if code == responseCode {
			return nil
		}
	}

	return fmt.Errorf("expected response code %d got %d", expectedResponseCodes, responseCode)
}

func (a *AmqpManagement) request(ctx context.Context, id string, body any, path string, method string,
	expectedResponseCodes []int) (map[string]any, error) {
	amqpMessage := &amqp.Message{
		Value: body,
	}

	s := commandReplyTo
	amqpMessage.Properties = &amqp.MessageProperties{
		ReplyTo:   &s,
		To:        &path,
		Subject:   &method,
		MessageID: &id,
	}

	opts := &amqp.SendOptions{Settled: true}

	err := a.sender.Send(ctx, amqpMessage, opts)
	if err != nil {
		return make(map[string]any), err
	}

	msg, err := a.receiver.Receive(ctx, nil)
	if err != nil {
		return make(map[string]any), err
	}

	err = a.receiver.AcceptMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if msg.Properties == nil {
		return make(map[string]any), fmt.Errorf("expected properties in the message")
	}

	if msg.Properties.CorrelationID == nil {
		return make(map[string]any), fmt.Errorf("expected correlation id in the message")
	}

	if msg.Properties.CorrelationID != id {
		return make(map[string]any), fmt.Errorf("expected correlation id %s got %s", id, msg.Properties.CorrelationID)
	}

	switch msg.Value.(type) {
	case map[string]interface{}:
		return msg.Value.(map[string]any), nil
	}

	responseCode, _ := strconv.Atoi(*msg.Properties.Subject)

	err = a.validateResponseCode(responseCode, expectedResponseCodes)
	if err != nil {
		return nil, err
	}

	if responseCode == responseCode404 {
		return nil, ErrDoesNotExist
	}

	return make(map[string]any), nil
}

func (a *AmqpManagement) DeclareQueue(ctx context.Context, specification IQueueSpecification) (*AmqpQueueInfo, error) {
	if specification == nil {
		return nil, fmt.Errorf("queue specification cannot be nil. You need to provide a valid IQueueSpecification")
	}

	amqpQueue := newAmqpQueue(a, specification.name())
	amqpQueue.AutoDelete(specification.isAutoDelete())
	// TODO: config tweak to record only exclusive queues
	amqpQueue.Exclusive(specification.isExclusive())
	amqpQueue.QueueType(specification.queueType())
	amqpQueue.Arguments(specification.buildArguments())
	info, err := amqpQueue.Declare(ctx)
	if err != nil {
		return nil, err
	}

	if !a.isRecovering.Load() && a.shouldAddQueueRecoveryRecord(specification) {
		recoveryRecord := queueRecoveryRecord{
			queueName: specification.name(),
			queueType: specification.queueType(),
		}
		if specification.queueType() == Classic {
			recoveryRecord.autoDelete = ptr(specification.isAutoDelete())
			recoveryRecord.exclusive = ptr(specification.isExclusive())
		}
		a.topologyRecoveryRecords.addQueueRecord(recoveryRecord)
	}

	return info, nil
}

func (a *AmqpManagement) DeleteQueue(ctx context.Context, name string) error {
	q := newAmqpQueue(a, name)
	err := q.Delete(ctx)
	if err != nil {
		return err
	}
	a.topologyRecoveryRecords.removeQueueRecord(name)
	a.topologyRecoveryRecords.removeBindingRecordByDestinationQueue(name)
	return nil
}

func (a *AmqpManagement) DeclareExchange(ctx context.Context, exchangeSpecification IExchangeSpecification) (*AmqpExchangeInfo, error) {
	if exchangeSpecification == nil {
		return nil, errors.New("exchange specification cannot be nil. You need to provide a valid IExchangeSpecification")
	}

	exchange := newAmqpExchange(a, exchangeSpecification.name())
	exchange.AutoDelete(exchangeSpecification.isAutoDelete())
	exchange.ExchangeType(exchangeSpecification.exchangeType())
	exchange.Arguments(exchangeSpecification.arguments())
	r, err := exchange.Declare(ctx)
	if err != nil {
		return nil, err
	}

	if !a.isRecovering.Load() && a.shouldAddExchangeRecoveryRecord(exchangeSpecification) {
		a.topologyRecoveryRecords.addExchangeRecord(exchangeRecoveryRecord{
			exchangeName: exchangeSpecification.name(),
			exchangeType: exchangeSpecification.exchangeType(),
			autoDelete:   exchangeSpecification.isAutoDelete(),
			arguments:    exchangeSpecification.arguments(),
		})
	}

	return r, nil
}

func (a *AmqpManagement) DeleteExchange(ctx context.Context, name string) error {
	e := newAmqpExchange(a, name)
	err := e.Delete(ctx)
	if err != nil {
		return err
	}
	a.topologyRecoveryRecords.removeExchangeRecord(name)
	a.topologyRecoveryRecords.removeBindingRecordBySourceExchange(name)
	return nil
}

func (a *AmqpManagement) Bind(ctx context.Context, bindingSpecification IBindingSpecification) (string, error) {
	if bindingSpecification == nil {
		return "", fmt.Errorf("binding specification cannot be nil. You need to provide a valid IBindingSpecification")
	}

	bind := newAMQPBinding(a)
	bind.SourceExchange(bindingSpecification.sourceExchange())
	bind.Destination(bindingSpecification.destination(), bindingSpecification.isDestinationQueue())
	bind.BindingKey(bindingSpecification.bindingKey())
	bind.Arguments(bindingSpecification.arguments())
	r, err := bind.Bind(ctx)
	if err != nil {
		return "", err
	}

	if !a.isRecovering.Load() && a.shouldAddBindingRecoveryRecord(bindingSpecification) {
		a.topologyRecoveryRecords.addBindingRecord(bindingRecoveryRecord{
			sourceExchange:     bindingSpecification.sourceExchange(),
			destination:        bindingSpecification.destination(),
			isDestinationQueue: bindingSpecification.isDestinationQueue(),
			bindingKey:         bindingSpecification.bindingKey(),
			arguments:          bindingSpecification.arguments(),
			path:               r,
		})
	}

	return r, nil
}

func (a *AmqpManagement) Unbind(ctx context.Context, path string) error {
	bind := newAMQPBinding(a)
	err := bind.Unbind(ctx, path)
	if err != nil {
		return err
	}
	a.topologyRecoveryRecords.removeBindingRecord(path)
	return nil
}

func (a *AmqpManagement) QueueInfo(ctx context.Context, queueName string) (*AmqpQueueInfo, error) {
	path, err := queueAddress(&queueName)
	if err != nil {
		return nil, err
	}
	result, err := a.Request(ctx, amqp.Null{}, path, commandGet, []int{responseCode200, responseCode404})
	if err != nil {
		return nil, err
	}
	return newAmqpQueueInfo(result), nil
}

// PurgeQueue purges the queue
// returns the number of messages purged
func (a *AmqpManagement) PurgeQueue(ctx context.Context, name string) (int, error) {
	purge := newAmqpQueue(a, name)
	return purge.Purge(ctx)
}

func (a *AmqpManagement) refreshToken(ctx context.Context, token string) error {
	_, err := a.Request(ctx, []byte(token), authTokens, commandPut, []int{responseCode204})
	return err
}

func (a *AmqpManagement) NotifyStatusChange(channel chan *StateChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpManagement) State() ILifeCycleState {
	return a.lifeCycle.State()
}

func (a *AmqpManagement) shouldAddQueueRecoveryRecord(specification IQueueSpecification) bool {
	isTransient := specification.isExclusive() || specification.isAutoDelete()
	return a.topologyRecoveryOptions == TopologyRecoveryAllEnabled ||
		(a.topologyRecoveryOptions == TopologyRecoveryOnlyTransient && isTransient)
}

func (a *AmqpManagement) shouldAddExchangeRecoveryRecord(specification IExchangeSpecification) bool {
	isTransient := specification.isAutoDelete()
	return a.topologyRecoveryOptions == TopologyRecoveryAllEnabled ||
		(a.topologyRecoveryOptions == TopologyRecoveryOnlyTransient && isTransient)
}

func (a *AmqpManagement) shouldAddBindingRecoveryRecord(specification IBindingSpecification) bool {
	if a.topologyRecoveryOptions == TopologyRecoveryAllEnabled {
		return true
	}
	if a.topologyRecoveryOptions == TopologyRecoveryOnlyTransient {
		if a.isExchangeSourceForBindingTransient(specification) || a.isQueueDestinationForBindingTransient(specification) {
			return true
		}
	}
	return false
}

func (a *AmqpManagement) isQueueDestinationForBindingTransient(binding IBindingSpecification) bool {
	if !binding.isDestinationQueue() {
		return false
	}
	return slices.ContainsFunc(a.topologyRecoveryRecords.queues, func(queue queueRecoveryRecord) bool {
		isTransient := (queue.autoDelete != nil && *queue.autoDelete) || (queue.exclusive != nil && *queue.exclusive)
		return queue.queueName == binding.destination() && isTransient
	})
}

func (a *AmqpManagement) isExchangeSourceForBindingTransient(binding IBindingSpecification) bool {
	result := slices.ContainsFunc(a.topologyRecoveryRecords.exchanges, func(exchange exchangeRecoveryRecord) bool {
		isTransient := exchange.autoDelete
		return exchange.exchangeName == binding.sourceExchange() && isTransient
	})
	return result
}
