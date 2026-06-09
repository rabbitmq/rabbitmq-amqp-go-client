package rabbitmqamqp

import (
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/Azure/go-amqp"
)

const AtMostOnce = 0
const AtLeastOnce = 1

// senderLinkOptions returns the options for a sender link
// with the given address and link name.
// That should be the same for all the links.
func createSenderLinkOptions(address string, linkName string, deliveryMode int) *amqp.SenderOptions {
	prop := make(map[string]any)
	prop["paired"] = true
	sndSettleMode := amqp.SenderSettleModeSettled.Ptr()
	/// SndSettleMode = deliveryMode == DeliveryMode.AtMostOnce
	//                    ? SenderSettleMode.Settled
	//                    : SenderSettleMode.Unsettled,

	if deliveryMode == AtLeastOnce {
		sndSettleMode = amqp.SenderSettleModeUnsettled.Ptr()
	}

	return &amqp.SenderOptions{
		SourceAddress:               address,
		DynamicAddress:              false,
		ExpiryPolicy:                amqp.ExpiryPolicyLinkDetach,
		ExpiryTimeout:               0,
		Name:                        linkName,
		Properties:                  prop,
		SettlementMode:              sndSettleMode,
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeFirst.Ptr(),
	}
}

// receiverLinkOptions returns the options for a receiver link
// with the given address and link name.
// That should be the same for all the links.
func createReceiverLinkOptions(address string, options IConsumerOptions, deliveryMode int) *amqp.ReceiverOptions {
	prop := make(map[string]any)
	prop["paired"] = true

	// Check if pre-settled mode is enabled
	preSettled := getPreSettled(options)

	var receiverSettleMode *amqp.SenderSettleMode
	var settlementMode *amqp.ReceiverSettleMode

	if preSettled {
		// Pre-settled mode: AT_MOST_ONCE with auto-settle
		receiverSettleMode = amqp.SenderSettleModeSettled.Ptr()
		settlementMode = amqp.ReceiverSettleModeSecond.Ptr() // auto-settle
	} else {
		// Normal mode: use the provided deliveryMode
		receiverSettleMode = amqp.SenderSettleModeSettled.Ptr()
		if deliveryMode == AtLeastOnce {
			receiverSettleMode = amqp.SenderSettleModeUnsettled.Ptr()
		}
		settlementMode = amqp.ReceiverSettleModeFirst.Ptr() // explicit settle
	}

	result := &amqp.ReceiverOptions{
		TargetAddress:             address,
		DynamicAddress:            false,
		Name:                      getLinkName(options),
		Properties:                prop,
		Durability:                0,
		ExpiryTimeout:             0,
		SettlementMode:            settlementMode,
		RequestedSenderSettleMode: receiverSettleMode,
		ExpiryPolicy:              amqp.ExpiryPolicyLinkDetach,
		Credit:                    getInitialCredits(options),
		Filters:                   getLinkFilters(options),
	}
	return result
}

func createDynamicReceiverLinkOptions(options IConsumerOptions) *amqp.ReceiverOptions {
	prop := make(map[string]any)
	prop["paired"] = true

	return &amqp.ReceiverOptions{
		Name:                      getLinkName(options),
		SourceCapabilities:        []string{"rabbitmq:volatile-queue"},
		SourceExpiryPolicy:        amqp.ExpiryPolicyLinkDetach,
		DynamicAddress:            true,
		RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
		Credit:                    getInitialCredits(options),
		Filters:                   getLinkFilters(options),
	}
}

// setConsumerTimeoutProperty injects the rabbitmq:consumer-timeout attach property (milliseconds)
// when ConsumerOptions.ConsumerTimeout is set. This is only supported on quorum and JMS queues
// and requires RabbitMQ 4.3 or later.
func setConsumerTimeoutProperty(opts *amqp.ReceiverOptions, options IConsumerOptions) {
	if opts == nil || options == nil {
		return
	}
	co, ok := options.(*ConsumerOptions)
	if !ok || co.ConsumerTimeout <= 0 {
		return
	}
	if opts.Properties == nil {
		opts.Properties = make(map[string]any)
	}
	opts.Properties[rabbitmqConsumerTimeoutProperty] = uint64(co.ConsumerTimeout.Milliseconds())
}

// setDeliveryReleaseHandler wires up ReceiverOptions.OnDeliveryStateChanged when
// ConsumerOptions.OnDeliveryRelease is set. The callback is dispatched to a new
// goroutine so the caller can safely call blocking operations such as Accept().
func setDeliveryReleaseHandler(opts *amqp.ReceiverOptions, options IConsumerOptions, c *Consumer) {
	if opts == nil || c == nil || options == nil {
		return
	}
	co, ok := options.(*ConsumerOptions)
	if !ok || co.OnDeliveryRelease == nil {
		return
	}
	handler := co.OnDeliveryRelease
	opts.OnDeliveryStateChanged = func(msg *amqp.Message, state amqp.DeliveryState) {
		if _, released := state.(*amqp.StateReleased); !released {
			return
		}
		receiver := c.receiver.Load()
		if receiver == nil {
			return
		}
		consumeCtx := c.buildConsumeContext(msg)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					Error("OnDeliveryRelease handler panicked", "recover", r)
				}
			}()
			timedOutCtx := &TimedOutDeliveryContext{
				receiver:         receiver,
				message:          msg,
				metricsCollector: c.connection.metricsCollector,
				consumeCtx:       consumeCtx,
			}
			handler(timedOutCtx, msg)
		}()
	}
}

func random(max int) int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return r.Intn(max)
}

func validateMessageAnnotations(annotations amqp.Annotations) error {
	for k := range annotations {
		switch tp := k.(type) {
		case string:
			if err := validateMessageAnnotationKey(tp); err != nil {
				return err
			}
		default:
			return fmt.Errorf("message annotation key must be a string: %v", k)
		}
	}
	return nil
}

func validateMessageAnnotationKey(key string) error {
	if key[:2] != "x-" {
		return fmt.Errorf("message annotation key must start with 'x-': %s", key)
	}
	return nil
}

// url decode path segments
func decodePathSegments(segment string) (string, error) {
	decoded, err := url.PathUnescape(segment)
	if err != nil {
		return "", err
	}
	return decoded, nil
}

// remove /queues/ prefix from the queue address
func trimQueueAddress(address string) (string, error) {
	prefix := "/queues/"
	if len(address) < len(prefix) || address[:len(prefix)] != prefix {
		return "", fmt.Errorf("invalid queue address: %s", address)
	}
	return address[len(prefix):], nil
}

// trim and decode queue name from the queue address
func parseQueueAddress(address string) (string, error) {
	trimmed, err := trimQueueAddress(address)
	if err != nil {
		return "", err
	}
	decoded, err := decodePathSegments(trimmed)
	if err != nil {
		return "", err
	}
	return decoded, nil
}
