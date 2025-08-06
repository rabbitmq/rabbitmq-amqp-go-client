package rabbitmqamqp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"
)

func generateNameWithDateTime(name string) string {
	return fmt.Sprintf("%s_%s", name, strconv.FormatInt(time.Now().Unix(), 10))
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

func uint32Ptr(i uint32) *uint32 {
	return &i
}

// create a static date time string for testing

func createDateTime() time.Time {
	layout := time.RFC3339
	value := "2006-01-02T15:04:05Z"
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

// convert time to pointer
func timePtr(t time.Time) *time.Time {
	return &t
}

// ptr returns a pointer to the given value of type T
func ptr[T any](v T) *T {
	return &v
}

type GinkgoLogHandler struct {
	slog.Handler
	w io.Writer
}

func (h *GinkgoLogHandler) Handle(_ context.Context, r slog.Record) error {
	_, err := h.w.Write([]byte(r.Message + "\n"))
	return err
}

func NewGinkgoHandler(level slog.Level, writer io.Writer) slog.Handler {
	handlerOptions := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	return &GinkgoLogHandler{
		Handler: slog.NewJSONHandler(os.Stdout, handlerOptions),
		w:       writer,
	}
}

func declareQueueAndConnection(name string) (*AmqpConnection, error) {
	connection, err := Dial(context.Background(), "amqp://", nil)
	if err != nil {
		return nil, err
	}
	_, err = connection.Management().DeclareQueue(context.Background(), &ClassicQueueSpecification{Name: name, IsAutoDelete: true})
	if err != nil {
		return nil, err
	}
	return connection, nil
}
