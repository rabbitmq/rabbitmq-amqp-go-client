package rabbitmq_amqp

import (
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"strings"
)

// TargetAddress is an interface that represents an address that can be used to send messages to.
// It can be either a Queue or an Exchange with a routing key.
type TargetAddress interface {
	toAddress() (string, error)
}

// QueueAddress represents the address of a queue.
type QueueAddress struct {
	Queue      string // The name of the queue
	Parameters string // Additional parameters not related to the queue. Most of the time it is empty
}

func (qas *QueueAddress) toAddress() (string, error) {
	q := &qas.Queue
	if isStringNilOrEmpty(&qas.Queue) {
		q = nil
	}
	return queueAddress(q)
}

// ExchangeAddress represents the address of an exchange with a routing key.
type ExchangeAddress struct {
	Exchange string // The name of the exchange
	Key      string // The routing key. Can be empty
}

func (eas *ExchangeAddress) toAddress() (string, error) {
	ex := &eas.Exchange
	if isStringNilOrEmpty(&eas.Exchange) {
		ex = nil
	}
	k := &eas.Key
	if isStringNilOrEmpty(&eas.Key) {
		k = nil
	}
	return exchangeAddress(ex, k)
}

// MessageToAddressHelper sets the To property of the message to the address of the target.
// The target must be a QueueAddress or an ExchangeAddress.
// Note: The field To will be overwritten if it is already set.
func MessageToAddressHelper(msgRef *amqp.Message, target TargetAddress) error {
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

// address Creates the address for the exchange or queue following the RabbitMQ conventions.
// see: https://www.rabbitmq.com/docs/next/amqp#address-v2
func address(exchange, key, queue *string, urlParameters *string) (string, error) {
	if exchange == nil && queue == nil {
		return "", errors.New("exchange or queue must be set")
	}

	urlAppend := ""
	if !isStringNilOrEmpty(urlParameters) {
		urlAppend = *urlParameters
	}
	if !isStringNilOrEmpty(exchange) && !isStringNilOrEmpty(queue) {
		return "", errors.New("exchange and queue cannot be set together")
	}

	if !isStringNilOrEmpty(exchange) {
		if !isStringNilOrEmpty(key) {
			return "/" + exchanges + "/" + encodePathSegments(*exchange) + "/" + encodePathSegments(*key) + urlAppend, nil
		}
		return "/" + exchanges + "/" + encodePathSegments(*exchange) + urlAppend, nil
	}

	if queue == nil {
		return "", nil
	}

	if isStringNilOrEmpty(queue) {
		return "", errors.New("queue must be set")
	}

	return "/" + queues + "/" + encodePathSegments(*queue) + urlAppend, nil
}

// exchangeAddress Creates the address for the exchange
// See address for more information
func exchangeAddress(exchange, key *string) (string, error) {
	return address(exchange, key, nil, nil)
}

// queueAddress Creates the address for the queue.
// See address for more information
func queueAddress(queue *string) (string, error) {
	return address(nil, nil, queue, nil)
}

// PurgeQueueAddress Creates the address for purging the queue.
// See address for more information
func purgeQueueAddress(queue *string) (string, error) {
	parameter := "/messages"
	return address(nil, nil, queue, &parameter)
}

// encodePathSegments takes a string and returns its percent-encoded representation.
func encodePathSegments(input string) string {
	var encoded strings.Builder

	// Iterate over each character in the input string
	for _, char := range input {
		// Check if the character is an unreserved character (i.e., it doesn't need encoding)
		if isUnreserved(char) {
			encoded.WriteRune(char) // Append as is
		} else {
			// Encode character To %HH format
			encoded.WriteString(fmt.Sprintf("%%%02X", char))
		}
	}

	return encoded.String()
}

//// Decode takes a percent-encoded string and returns its decoded representation.
//func decode(input string) (string, error) {
//	// Use url.QueryUnescape which properly decodes percent-encoded strings
//	decoded, err := url.QueryUnescape(input)
//	if err != nil {
//		return "", err
//	}
//
//	return decoded, nil
//}

// isUnreserved checks if a character is an unreserved character in percent encoding
// Unreserved characters are: A-Z, a-z, 0-9, -, ., _, ~
func isUnreserved(char rune) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '.' || char == '_' || char == '~'
}

func bindingPath() string {
	return "/" + bindings
}

func bindingPathWithExchangeQueueKey(toQueue bool, sourceName, destinationName, key string) string {
	sourceNameEncoded := encodePathSegments(sourceName)
	destinationNameEncoded := encodePathSegments(destinationName)
	keyEncoded := encodePathSegments(key)
	destinationType := "dste"
	if toQueue {
		destinationType = "dstq"
	}
	format := "/%s/src=%s;%s=%s;key=%s;args="
	return fmt.Sprintf(format, bindings, sourceNameEncoded, destinationType, destinationNameEncoded, keyEncoded)
}

func validateAddress(address string) bool {
	return strings.HasPrefix(address, fmt.Sprintf("/%s/", exchanges)) || strings.HasPrefix(address, fmt.Sprintf("/%s/", queues))
}
