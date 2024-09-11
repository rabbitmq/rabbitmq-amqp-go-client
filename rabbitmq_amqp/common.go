package rabbitmq_amqp

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/google/uuid"
	"net/url"
	"strings"
)

type PercentCodec struct{}

// Encode takes a string and returns its percent-encoded representation.
func (pc *PercentCodec) Encode(input string) string {
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
func (pc *PercentCodec) Decode(input string) (string, error) {
	// Use url.QueryUnescape which properly decodes percent-encoded strings
	decoded, err := url.QueryUnescape(input)
	if err != nil {
		return "", err
	}

	return decoded, nil
}

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
)

const (
	Exchanges = "exchanges"
	Key       = "key"
	Queues    = "queues"
	Bindings  = "bindings"
)

// isUnreserved checks if a character is an unreserved character in percent encoding
// Unreserved characters are: A-Z, a-z, 0-9, -, ., _, ~
func isUnreserved(char rune) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '.' || char == '_' || char == '~'
}

func encodePathSegments(pathSegments string) string {
	return (&PercentCodec{}).Encode(pathSegments)
}

func queuePath(queueName string) string {
	return "/" + Queues + "/" + encodePathSegments(queueName)
}

func exchangePath(exchangeName string) string {
	return "/" + Exchanges + "/" + encodePathSegments(exchangeName)
}

func validatePositive(label string, value int64) error {
	if value < 0 {
		return fmt.Errorf("value for %s must be positive, got %d", label, value)
	}
	return nil
}

func GenerateNameWithDefaultPrefix() string {
	return GenerateName("client.gen-")
}

// GenerateName generates a unique name with the given prefix
func GenerateName(prefix string) string {

	var uid = uuid.New()
	var uuidBytes = []byte(uid.String())
	var _md5 = md5.New()
	var digest = _md5.Sum(uuidBytes)
	result := base64.StdEncoding.EncodeToString(digest)
	result = strings.ReplaceAll(result, "+", "-")
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "=", "")
	return prefix + result

}
