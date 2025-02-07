package rabbitmq_amqp

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"math/rand"
	"time"
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
func createReceiverLinkOptions(address string, options ConsumerOptions, deliveryMode int) *amqp.ReceiverOptions {
	prop := make(map[string]any)
	prop["paired"] = true
	receiverSettleMode := amqp.SenderSettleModeSettled.Ptr()
	/// SndSettleMode = deliveryMode == DeliveryMode.AtMostOnce
	//                    ? SenderSettleMode.Settled
	//                    : SenderSettleMode.Unsettled,

	if deliveryMode == AtLeastOnce {
		receiverSettleMode = amqp.SenderSettleModeUnsettled.Ptr()
	}

	return &amqp.ReceiverOptions{
		TargetAddress:             address,
		DynamicAddress:            false,
		Name:                      getLinkName(options),
		Properties:                prop,
		Durability:                0,
		ExpiryTimeout:             0,
		SettlementMode:            amqp.ReceiverSettleModeFirst.Ptr(),
		RequestedSenderSettleMode: receiverSettleMode,
		ExpiryPolicy:              amqp.ExpiryPolicyLinkDetach,
		Credit:                    getInitialCredits(options),
	}
}

func random(max int) int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return r.Intn(max)
}

func validateMessageAnnotations(annotations amqp.Annotations) error {
	for k, _ := range annotations {
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
