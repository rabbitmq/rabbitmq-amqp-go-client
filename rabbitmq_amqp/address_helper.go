package rabbitmq_amqp

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Address Creates the address for the exchange or queue following the RabbitMQ conventions.
// see: https://www.rabbitmq.com/docs/next/amqp#address-v2
func Address(exchange, key, queue *string, urlParameters *string) (string, error) {
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

// ExchangeAddress Creates the address for the exchange
// See Address for more information
func ExchangeAddress(exchange, key *string) (string, error) {
	return Address(exchange, key, nil, nil)
}

// QueueAddress Creates the address for the queue.
// See Address for more information
func QueueAddress(queue *string) (string, error) {
	return Address(nil, nil, queue, nil)
}

// PurgeQueueAddress Creates the address for purging the queue.
// See Address for more information
func PurgeQueueAddress(queue *string) (string, error) {
	parameter := "/messages"
	return Address(nil, nil, queue, &parameter)
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

// Decode takes a percent-encoded string and returns its decoded representation.
func decode(input string) (string, error) {
	// Use url.QueryUnescape which properly decodes percent-encoded strings
	decoded, err := url.QueryUnescape(input)
	if err != nil {
		return "", err
	}

	return decoded, nil
}

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
