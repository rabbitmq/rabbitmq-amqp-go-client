package rabbitmq_amqp

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type AddressBuilder struct {
	queue    *string
	exchange *string
	key      *string
	append   *string
}

func NewAddressBuilder() *AddressBuilder {
	return &AddressBuilder{}
}

func (a *AddressBuilder) Queue(queue string) *AddressBuilder {
	a.queue = &queue
	return a
}

func (a *AddressBuilder) Exchange(exchange string) *AddressBuilder {
	a.exchange = &exchange
	return a
}

func (a *AddressBuilder) Key(key string) *AddressBuilder {
	a.key = &key
	return a
}

func (a *AddressBuilder) Append(append string) *AddressBuilder {
	a.append = &append
	return a
}

func (a *AddressBuilder) Address() (string, error) {
	if a.exchange == nil && a.queue == nil {
		return "", errors.New("exchange or queue must be set")
	}

	urlAppend := ""
	if !isStringNilOrEmpty(a.append) {
		urlAppend = *a.append
	}
	if !isStringNilOrEmpty(a.exchange) && !isStringNilOrEmpty(a.queue) {
		return "", errors.New("exchange and queue cannot be set together")
	}

	if !isStringNilOrEmpty(a.exchange) {
		if !isStringNilOrEmpty(a.key) {
			return "/" + exchanges + "/" + encodePathSegments(*a.exchange) + "/" + encodePathSegments(*a.key) + urlAppend, nil
		}
		return "/" + exchanges + "/" + encodePathSegments(*a.exchange) + urlAppend, nil
	}

	if a.queue == nil {
		return "", nil
	}

	if isStringNilOrEmpty(a.queue) {
		return "", errors.New("queue must be set")
	}

	return "/" + queues + "/" + encodePathSegments(*a.queue) + urlAppend, nil
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
