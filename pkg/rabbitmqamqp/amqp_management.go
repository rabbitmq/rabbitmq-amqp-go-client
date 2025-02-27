package rabbitmqamqp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
}

func NewAmqpManagement() *AmqpManagement {
	return &AmqpManagement{
		lifeCycle: NewLifeCycle(),
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
	amqpQueue.Exclusive(specification.isExclusive())
	amqpQueue.QueueType(specification.queueType())
	amqpQueue.SetArguments(specification.buildArguments())

	return amqpQueue.Declare(ctx)
}

func (a *AmqpManagement) DeleteQueue(ctx context.Context, name string) error {
	q := newAmqpQueue(a, name)
	return q.Delete(ctx)
}

func (a *AmqpManagement) DeclareExchange(ctx context.Context, exchangeSpecification IExchangeSpecification) (*AmqpExchangeInfo, error) {
	if exchangeSpecification == nil {
		return nil, errors.New("exchange specification cannot be nil. You need to provide a valid IExchangeSpecification")
	}

	exchange := newAmqpExchange(a, exchangeSpecification.name())
	exchange.AutoDelete(exchangeSpecification.isAutoDelete())
	exchange.ExchangeType(exchangeSpecification.exchangeType())
	return exchange.Declare(ctx)
}

func (a *AmqpManagement) DeleteExchange(ctx context.Context, name string) error {
	e := newAmqpExchange(a, name)
	return e.Delete(ctx)
}

func (a *AmqpManagement) Bind(ctx context.Context, bindingSpecification IBindingSpecification) (string, error) {
	if bindingSpecification == nil {
		return "", fmt.Errorf("binding specification cannot be nil. You need to provide a valid IBindingSpecification")
	}

	bind := newAMQPBinding(a)
	bind.SourceExchange(bindingSpecification.sourceExchange())
	bind.Destination(bindingSpecification.destination(), bindingSpecification.isDestinationQueue())
	bind.BindingKey(bindingSpecification.bindingKey())
	return bind.Bind(ctx)

}
func (a *AmqpManagement) Unbind(ctx context.Context, path string) error {
	bind := newAMQPBinding(a)
	return bind.Unbind(ctx, path)
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

func (a *AmqpManagement) NotifyStatusChange(channel chan *StateChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpManagement) State() ILifeCycleState {
	return a.lifeCycle.State()
}
