package rabbitmqamqp

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/google/uuid"
	"strings"
)

// public consts

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
	uuidBytes := []byte(uid.String())
	md5obj := md5.New()
	digest := md5obj.Sum(uuidBytes)
	result := base64.StdEncoding.EncodeToString(digest)
	result = strings.ReplaceAll(result, "+", "-")
	result = strings.ReplaceAll(result, "/", "_")
	result = strings.ReplaceAll(result, "=", "")
	return prefix + result
}

func isStringNilOrEmpty(str *string) bool {
	return str == nil || len(*str) == 0

}
