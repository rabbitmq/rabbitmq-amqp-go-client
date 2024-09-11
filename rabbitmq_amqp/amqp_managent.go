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

var PreconditionFailed = errors.New("precondition Failed")

type AmqpManagement struct {
	session   *amqp.Session
	sender    *amqp.Sender
	receiver  *amqp.Receiver
	lifeCycle *LifeCycle
	cancel    context.CancelFunc
}

func (a *AmqpManagement) Exchange(exchangeName string) IExchangeSpecification {
	return newAmqpExchange(a, exchangeName)
}

func NewAmqpManagement() *AmqpManagement {
	return &AmqpManagement{
		lifeCycle: NewLifeCycle(),
	}
}

func (a *AmqpManagement) ensureReceiverLink(ctx context.Context) error {
	if a.receiver == nil {
		prop := make(map[string]any)
		prop["paired"] = true
		opts := &amqp.ReceiverOptions{
			DynamicAddress:            false,
			Name:                      linkPairName,
			Properties:                prop,
			RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
			SettlementMode:            amqp.ReceiverSettleModeFirst.Ptr(),
			TargetAddress:             managementNodeAddress,
			ExpiryPolicy:              amqp.ExpiryPolicyLinkDetach,
			Credit:                    100,
		}
		receiver, err := a.session.NewReceiver(ctx, managementNodeAddress, opts)
		if err != nil {
			return err
		}
		a.receiver = receiver
		return nil
	}
	return nil
}

//func (a *AmqpManagement) processMessages(ctx context.Context) error {
//
//	go func() {
//
//		for a.GetStatus() == Open {
//			msg, err := a.receiver.Receive(ctx, nil) // blocking call
//			if err != nil {
//				fmt.Printf("Exiting processMessages %s\n", err)
//				return
//			}
//
//			if msg != nil {
//				a.receiver.AcceptMessage(ctx, msg)
//			}
//		}
//
//		fmt.Printf("Exiting processMessages\n")
//	}()

//return nil
//}

func (a *AmqpManagement) ensureSenderLink(ctx context.Context) error {
	if a.sender == nil {
		prop := make(map[string]any)
		prop["paired"] = true
		opts := &amqp.SenderOptions{
			DynamicAddress:              false,
			ExpiryPolicy:                amqp.ExpiryPolicyLinkDetach,
			ExpiryTimeout:               0,
			Name:                        linkPairName,
			Properties:                  prop,
			SettlementMode:              amqp.SenderSettleModeSettled.Ptr(),
			RequestedReceiverSettleMode: amqp.ReceiverSettleModeFirst.Ptr(),
			SourceAddress:               managementNodeAddress,
		}
		sender, err := a.session.NewSender(ctx, managementNodeAddress, opts)
		if err != nil {
			return err
		}

		a.sender = sender
		return nil
	}
	return nil
}

func (a *AmqpManagement) Open(ctx context.Context, connection IConnection) error {
	session, err := connection.(*AmqpConnection).Connection.NewSession(ctx, nil)
	if err != nil {
		return err
	}
	a.session = session
	err = a.ensureSenderLink(ctx)

	if err != nil {
		return err
	}

	time.Sleep(500 * time.Millisecond)
	err = a.ensureReceiverLink(ctx)
	time.Sleep(500 * time.Millisecond)
	if err != nil {
		return err
	}
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
		return PreconditionFailed
	}

	for _, code := range expectedResponseCodes {
		if code == responseCode {
			return nil
		}
	}

	return PreconditionFailed
}

func (a *AmqpManagement) request(ctx context.Context, id string, body any, path string, method string,
	expectedResponseCodes []int) (map[string]any, error) {
	amqpMessage := amqp.NewMessageWithValue(body)
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

	i, _ := strconv.Atoi(*msg.Properties.Subject)

	err = a.validateResponseCode(i, expectedResponseCodes)
	if err != nil {
		return nil, err
	}

	return make(map[string]any), nil
}

func (a *AmqpManagement) Queue(queueName string) IQueueSpecification {
	return newAmqpQueue(a, queueName)
}

func (a *AmqpManagement) QueueClientName() IQueueSpecification {
	return newAmqpQueue(a, "")
}

func (a *AmqpManagement) NotifyStatusChange(channel chan *StatusChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpManagement) GetStatus() int {
	return a.lifeCycle.Status()
}
