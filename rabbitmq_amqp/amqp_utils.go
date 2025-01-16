package rabbitmq_amqp

import (
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
func createReceiverLinkOptions(address string, linkName string) *amqp.ReceiverOptions {
	prop := make(map[string]any)
	prop["paired"] = true
	return &amqp.ReceiverOptions{
		TargetAddress:             address,
		DynamicAddress:            false,
		Name:                      linkName,
		Properties:                prop,
		RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
		SettlementMode:            amqp.ReceiverSettleModeFirst.Ptr(),
		ExpiryPolicy:              amqp.ExpiryPolicyLinkDetach,
		Credit:                    100,
	}
}

func random(max int) int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return r.Intn(max)
}
