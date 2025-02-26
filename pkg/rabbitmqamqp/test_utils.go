package rabbitmqamqp

import (
	"fmt"
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
