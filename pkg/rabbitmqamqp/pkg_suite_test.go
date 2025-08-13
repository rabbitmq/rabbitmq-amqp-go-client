package rabbitmqamqp_test

import (
	"log/slog"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

func TestPkg(t *testing.T) {
	rabbitmqamqp.SetSlogHandler(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pkg Suite")
}
