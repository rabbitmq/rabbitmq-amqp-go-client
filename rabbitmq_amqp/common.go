package rabbitmq_amqp

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

const (
	responseCode200       = 200
	responseCode201       = 201
	responseCode204       = 204
	responseCode409       = 409
	commandPut            = "PUT"
	commandGet            = "GET"
	commandPost           = "POST"
	commandDelete         = "DELETE"
	commandReplyTo        = "$me"
	managementNodeAddress = "/management"
	linkPairName          = "management-link-pair"
	exchanges             = "exchanges"
	key                   = "key"
	queues                = "queues"
	bindings              = "bindings"
)

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

func queuePath(queueName string) string {
	return "/" + queues + "/" + encodePathSegments(queueName)
}

func queuePurgePath(queueName string) string {
	return "/" + queues + "/" + encodePathSegments(queueName) + "/messages"
}

func exchangePath(exchangeName string) string {
	return "/" + exchanges + "/" + encodePathSegments(exchangeName)
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

func validatePositive(label string, value int64) error {
	if value < 0 {
		return fmt.Errorf("value for %s must be positive, got %d", label, value)
	}
	return nil
}

func generateNameWithDefaultPrefix() string {
	return generateName("client.gen-")
}

// generateName generates a unique name with the given prefix
func generateName(prefix string) string {
	uid := uuid.New()
	uuidBytes := []byte(uid.String())
	md5obj := md5.New()
	digest := md5obj.Sum(uuidBytes)
	result := base64.StdEncoding.EncodeToString(digest)
	result = strings.ReplaceAll(result, "+", "-")
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "=", "")
	return prefix + result
}
