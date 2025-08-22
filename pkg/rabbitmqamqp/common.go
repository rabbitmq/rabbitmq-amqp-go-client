package rabbitmqamqp

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// public consts

const AMQPS = "amqps"
const StreamFilterValue = "x-stream-filter-value"

const (
	responseCode200       = 200
	responseCode201       = 201
	responseCode204       = 204
	responseCode404       = 404
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
	authTokens            = "/auth/tokens"
)

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
	hasher := md5.New()
	hasher.Write([]byte(uid.String()))
	digest := hasher.Sum(nil)
	result := base64.StdEncoding.EncodeToString(digest)
	replacer := strings.NewReplacer("+", "-", "/", "_", "=", "")
	return prefix + replacer.Replace(result)
}

func isStringNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
}
