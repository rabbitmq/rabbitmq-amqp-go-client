package rabbitmqamqp

import (
	"errors"
	"github.com/Azure/go-amqp"
)

// MessagePropertyToAddress sets the To property of the message to the address of the target.
// The target must be a QueueAddress or an ExchangeAddress.
// Note: The field msgRef.Properties.To will be overwritten if it is already set.
func MessagePropertyToAddress(msgRef *amqp.Message, target ITargetAddress) error {
	if target == nil {
		return errors.New("target cannot be nil")
	}

	address, err := target.toAddress()
	if err != nil {
		return err
	}

	if msgRef.Properties == nil {
		msgRef.Properties = &amqp.MessageProperties{}
	}
	msgRef.Properties.To = &address
	return nil
}

// NewMessage creates a new AMQP 1.0 message with the given payload.
func NewMessage(body []byte) *amqp.Message {
	return amqp.NewMessage(body)
}

// NewMessageWithAddress creates a new AMQP 1.0  new message with the given payload and sets the To property to the address of the target.
// The target must be a QueueAddress or an ExchangeAddress.
// This function is a helper that combines NewMessage and MessagePropertyToAddress.
func NewMessageWithAddress(body []byte, target ITargetAddress) (*amqp.Message, error) {
	message := amqp.NewMessage(body)
	err := MessagePropertyToAddress(message, target)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// NewMessageWithFilter creates a new AMQP 1.0 message with the given payload and sets the
// StreamFilterValue property to the filter value.
func NewMessageWithFilter(body []byte, filter string) *amqp.Message {
	msg := amqp.NewMessage(body)
	msg.Annotations = amqp.Annotations{
		// here we set the filter value taken from the filters array
		StreamFilterValue: filter,
	}
	return msg
}
