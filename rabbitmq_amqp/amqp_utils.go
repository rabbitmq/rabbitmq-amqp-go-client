package rabbitmq_amqp

import "github.com/Azure/go-amqp"

// senderLinkOptions returns the options for a sender link
// with the given address and link name.
// That should be the same for all the links.
func createSenderLinkOptions(address string, linkName string) *amqp.SenderOptions {
	prop := make(map[string]any)
	prop["paired"] = true
	return &amqp.SenderOptions{
		DynamicAddress:              false,
		ExpiryPolicy:                amqp.ExpiryPolicyLinkDetach,
		ExpiryTimeout:               0,
		Name:                        linkName,
		Properties:                  prop,
		SettlementMode:              amqp.SenderSettleModeSettled.Ptr(),
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeFirst.Ptr(),
		SourceAddress:               address,
	}
}

// receiverLinkOptions returns the options for a receiver link
// with the given address and link name.
// That should be the same for all the links.
func createReceiverLinkOptions(address string, linkName string) *amqp.ReceiverOptions {
	prop := make(map[string]any)
	prop["paired"] = true
	return &amqp.ReceiverOptions{
		DynamicAddress:            false,
		Name:                      linkName,
		Properties:                prop,
		RequestedSenderSettleMode: amqp.SenderSettleModeSettled.Ptr(),
		SettlementMode:            amqp.ReceiverSettleModeFirst.Ptr(),
		TargetAddress:             address,
		ExpiryPolicy:              amqp.ExpiryPolicyLinkDetach,
		Credit:                    100,
	}
}
