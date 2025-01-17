package rabbitmq_amqp

import "fmt"

type Annotations map[string]any

func validateMessageAnnotations(annotations map[string]any) error {
	for key := range annotations {
		return validateMessageAnnotationKey(key)
	}
	return nil
}

func validateMessageAnnotationKey(key string) error {
	if key[:2] != "x-" {
		return fmt.Errorf("message annotation key must start with 'x-': %s", key)
	}
	return nil
}
