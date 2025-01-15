package rabbitmq_amqp

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"strconv"
	"time"
)

var ErrPreconditionFailed = errors.New("precondition Failed")
var ErrDoesNotExist = errors.New("does not exist")

type AmqpManagement struct {
	session   *amqp.Session
	sender    *amqp.Sender
	receiver  *amqp.Receiver
	lifeCycle *LifeCycle
	cancel    context.CancelFunc
}

func NewAmqpManagement() *AmqpManagement {
	return &AmqpManagement{
		lifeCycle: NewLifeCycle(),
	}
}

func (a *AmqpManagement) ensureReceiverLink(ctx context.Context) error {
	if a.receiver == nil {
		opts := createReceiverLinkOptions(managementNodeAddress, linkPairName)
		receiver, err := a.session.NewReceiver(ctx, managementNodeAddress, opts)
		if err != nil {
			return err
		}
		a.receiver = receiver
		return nil
	}
	return nil
}

func (a *AmqpManagement) ensureSenderLink(ctx context.Context) error {
	if a.sender == nil {
		sender, err := a.session.NewSender(ctx, managementNodeAddress,
			createSenderLinkOptions(managementNodeAddress, linkPairName))
		if err != nil {
			return err
		}

		a.sender = sender
		return nil
	}
	return nil
}

func (a *AmqpManagement) Open(ctx context.Context, connection *AmqpConnection) error {
	session, err := connection.Connection.NewSession(ctx, nil)
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

	a.lifeCycle.SetStatus(Open)
	return ctx.Err()
}

func (a *AmqpManagement) Close(ctx context.Context) error {
	_ = a.sender.Close(ctx)
	_ = a.receiver.Close(ctx)
	err := a.session.Close(ctx)
	a.lifeCycle.SetStatus(Closed)
	return err
}

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

func (a *AmqpManagement) DeclareQueue(ctx context.Context, specification *QueueSpecification) (*AmqpQueueInfo, error) {
	var amqpQueue *AmqpQueue

	if specification == nil || len(specification.Name) <= 0 {
		// If the specification is nil or the name is empty, then we create a new queue
		// with a random name with generateNameWithDefaultPrefix()
		amqpQueue = newAmqpQueue(a, "")
	} else {
		amqpQueue = newAmqpQueue(a, specification.Name)
		amqpQueue.AutoDelete(specification.IsAutoDelete)
		amqpQueue.Exclusive(specification.IsExclusive)
		amqpQueue.MaxLengthBytes(specification.MaxLengthBytes)
		amqpQueue.DeadLetterExchange(specification.DeadLetterExchange)
		amqpQueue.DeadLetterRoutingKey(specification.DeadLetterRoutingKey)
		amqpQueue.QueueType(specification.QueueType)
	}

	return amqpQueue.Declare(ctx)
}

func (a *AmqpManagement) DeleteQueue(ctx context.Context, name string) error {
	q := newAmqpQueue(a, name)
	return q.Delete(ctx)
}

func (a *AmqpManagement) DeclareExchange(ctx context.Context, exchangeSpecification *ExchangeSpecification) (*AmqpExchangeInfo, error) {
	if exchangeSpecification == nil {
		return nil, fmt.Errorf("exchangeSpecification is nil")
	}

	exchange := newAmqpExchange(a, exchangeSpecification.Name)
	exchange.AutoDelete(exchangeSpecification.IsAutoDelete)
	exchange.ExchangeType(exchangeSpecification.ExchangeType)
	return exchange.Declare(ctx)
}

func (a *AmqpManagement) DeleteExchange(ctx context.Context, name string) error {
	e := newAmqpExchange(a, name)
	return e.Delete(ctx)
}

func (a *AmqpManagement) Bind(ctx context.Context, bindingSpecification *BindingSpecification) (string, error) {
	bind := newAMQPBinding(a)
	bind.SourceExchange(bindingSpecification.SourceExchange)
	bind.DestinationQueue(bindingSpecification.DestinationQueue)
	bind.DestinationExchange(bindingSpecification.DestinationExchange)
	bind.BindingKey(bindingSpecification.BindingKey)
	return bind.Bind(ctx)

}
func (a *AmqpManagement) Unbind(ctx context.Context, bindingPath string) error {
	bind := newAMQPBinding(a)
	return bind.Unbind(ctx, bindingPath)
}
func (a *AmqpManagement) QueueInfo(ctx context.Context, queueName string) (*AmqpQueueInfo, error) {
	path, err := QueueAddress(&queueName)
	if err != nil {
		return nil, err
	}
	result, err := a.Request(ctx, amqp.Null{}, path, commandGet, []int{responseCode200, responseCode404})
	if err != nil {
		return nil, err
	}
	return newAmqpQueueInfo(result), nil
}

func (a *AmqpManagement) PurgeQueue(ctx context.Context, queueName string) (int, error) {
	purge := newAmqpQueue(a, queueName)
	return purge.Purge(ctx)
}

func (a *AmqpManagement) NotifyStatusChange(channel chan *StatusChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpManagement) Status() int {
	return a.lifeCycle.Status()
}
