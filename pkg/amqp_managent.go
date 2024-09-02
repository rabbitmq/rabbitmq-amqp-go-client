package pkg

import (
	"context"
	"github.com/Azure/go-amqp"
	"time"
)

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

func (a *AmqpManagement) processMessages(ctx context.Context) error {

	go func() {
		msg, err := a.receiver.Receive(ctx, nil)
		if err != nil {
			return
		}

		if msg != nil {
			a.receiver.AcceptMessage(ctx, msg)
		}
	}()

	return nil
}

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

	err = a.processMessages(ctx)

	if err != nil {
		return err
	}
	a.lifeCycle.SetStatus(Open)
	return err
}

func (a *AmqpManagement) Close(ctx context.Context) error {
	err := a.session.Close(ctx)
	a.lifeCycle.SetStatus(Closed)
	return err
}

func (a *AmqpManagement) Queue(queueName string) IQueueSpecification {
	//TODO implement me
	panic("implement me")
}

func (a *AmqpManagement) Request(ctx context.Context, id string, body any, path string, method string,
	expectedResponseCodes []int) error {
	amqpMessage := amqp.NewMessage(nil)
	amqpMessage.Value = body
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
		return err
	}

	return nil
}

func (a *AmqpManagement) NotifyStatusChange(channel chan *StatusChanged) {
	a.lifeCycle.chStatusChanged = channel
}

func (a *AmqpManagement) GetStatus() int {
	return a.lifeCycle.Status()
}
