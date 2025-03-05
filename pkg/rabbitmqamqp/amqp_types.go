package rabbitmqamqp

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// the following types are alias to the go-amqp package

type DeliveryState = amqp.DeliveryState
type StateAccepted = amqp.StateAccepted
type StateRejected = amqp.StateRejected
type StateReleased = amqp.StateReleased
type StateModified = amqp.StateModified

type iLinkerName interface {
	linkName() string
}

func getLinkName(l iLinkerName) string {
	if l == nil || l.linkName() == "" {
		return uuid.New().String()
	}
	return l.linkName()
}

/// IConsumerOptions interface for the AMQP and Stream consumer///

type IConsumerOptions interface {
	// linkName returns the name of the link
	// if not set it will return a random UUID
	linkName() string
	// initialCredits returns the initial credits for the link
	// if not set it will return 256
	initialCredits() int32

	// linkFilters returns the link filters for the link.
	// It is mostly used for the stream consumers.
	linkFilters() []amqp.LinkFilter

	// id returns the id of the consumer
	id() string

	// validate the consumer options based on the available features
	validate(available *featuresAvailable) error
}

func getInitialCredits(co IConsumerOptions) int32 {
	if co == nil || co.initialCredits() == 0 {
		return 256
	}
	return co.initialCredits()
}

func getLinkFilters(co IConsumerOptions) []amqp.LinkFilter {
	if co == nil {
		return nil
	}
	return co.linkFilters()
}

type managementOptions struct {
}

func (mo *managementOptions) linkName() string {
	return linkPairName
}

func (mo *managementOptions) initialCredits() int32 {
	// by default i 256 but here we set it to 100. For the management is enough.
	return 100
}

func (mo *managementOptions) linkFilters() []amqp.LinkFilter {
	return nil
}

func (mo *managementOptions) id() string {
	return "management"
}

func (mo *managementOptions) validate(available *featuresAvailable) error {
	return nil
}

// ConsumerOptions represents the options for quorum and classic queues
type ConsumerOptions struct {
	//ReceiverLinkName: see the IConsumerOptions interface
	ReceiverLinkName string
	//InitialCredits: see the IConsumerOptions interface
	InitialCredits int32
	// The id of the consumer
	Id string
}

func (aco *ConsumerOptions) linkName() string {
	return aco.ReceiverLinkName
}

func (aco *ConsumerOptions) initialCredits() int32 {
	return aco.InitialCredits
}

func (aco *ConsumerOptions) linkFilters() []amqp.LinkFilter {
	return nil
}

func (aco *ConsumerOptions) id() string {
	return aco.Id
}

func (aco *ConsumerOptions) validate(available *featuresAvailable) error {
	return nil
}

type IOffsetSpecification interface {
	toLinkFilter() amqp.LinkFilter
}

const rmqStreamFilter = "rabbitmq:stream-filter"
const rmqStreamOffsetSpec = "rabbitmq:stream-offset-spec"
const rmqStreamMatchUnfiltered = "rabbitmq:stream-match-unfiltered"
const amqpApplicationPropertiesFilter = "amqp:application-properties-filter"
const amqpPropertiesFilter = "amqp:properties-filter"

const offsetFirst = "first"
const offsetNext = "next"
const offsetLast = "last"

type OffsetFirst struct {
}

func (of *OffsetFirst) toLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetFirst)
}

type OffsetLast struct {
}

func (ol *OffsetLast) toLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetLast)
}

type OffsetValue struct {
	Offset uint64
}

func (ov *OffsetValue) toLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, ov.Offset)
}

type OffsetNext struct {
}

func (on *OffsetNext) toLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetNext)
}

// StreamFilterOptions represents the options that can be used to filter the stream data.
// It is used in the StreamConsumerOptions.
// See: https://www.rabbitmq.com/blog/2024/12/13/amqp-filter-expressions/
type StreamFilterOptions struct {
	// Filter values.
	Values []string
	//
	MatchUnfiltered bool

	// Filter the data based on Application Property
	ApplicationProperties map[string]any

	// Filter the data based on Message Properties
	Properties *amqp.MessageProperties
}

/*
StreamConsumerOptions represents the options for stream queues
It is mandatory in case of creating a stream consumer.
*/
type StreamConsumerOptions struct {
	//ReceiverLinkName: see the IConsumerOptions interface
	ReceiverLinkName string
	//InitialCredits: see the IConsumerOptions interface
	InitialCredits int32
	// The offset specification for the stream consumer
	// see the interface implementations
	Offset              IOffsetSpecification
	StreamFilterOptions *StreamFilterOptions

	Id string
}

func (sco *StreamConsumerOptions) linkName() string {
	return sco.ReceiverLinkName
}

func (sco *StreamConsumerOptions) initialCredits() int32 {
	return sco.InitialCredits
}

func (sco *StreamConsumerOptions) linkFilters() []amqp.LinkFilter {
	var filters []amqp.LinkFilter

	filters = append(filters, sco.Offset.toLinkFilter())
	if sco.StreamFilterOptions != nil && sco.StreamFilterOptions.Values != nil {
		var l []any
		for _, f := range sco.StreamFilterOptions.Values {
			l = append(l, f)
		}

		filters = append(filters, amqp.NewLinkFilter(rmqStreamFilter, 0, l))
		filters = append(filters, amqp.NewLinkFilter(rmqStreamMatchUnfiltered, 0, sco.StreamFilterOptions.MatchUnfiltered))
	}

	if sco.StreamFilterOptions != nil && sco.StreamFilterOptions.ApplicationProperties != nil {
		l := map[string]any{}
		for k, v := range sco.StreamFilterOptions.ApplicationProperties {
			l[k] = v
		}
		filters = append(filters, amqp.NewLinkFilter(amqpApplicationPropertiesFilter, 0, l))
	}

	if sco.StreamFilterOptions != nil && sco.StreamFilterOptions.Properties != nil {
		l := map[amqp.Symbol]any{}
		if sco.StreamFilterOptions.Properties.ContentType != nil {
			l["content-type"] = amqp.Symbol(*sco.StreamFilterOptions.Properties.ContentType)
		}

		if sco.StreamFilterOptions.Properties.ContentEncoding != nil {
			l["content-encoding"] = amqp.Symbol(*sco.StreamFilterOptions.Properties.ContentEncoding)
		}

		if sco.StreamFilterOptions.Properties.CorrelationID != nil {
			l["correlation-id"] = sco.StreamFilterOptions.Properties.CorrelationID
		}

		if sco.StreamFilterOptions.Properties.MessageID != nil {
			l["message-id"] = sco.StreamFilterOptions.Properties.MessageID
		}

		if sco.StreamFilterOptions.Properties.Subject != nil {
			l["subject"] = *sco.StreamFilterOptions.Properties.Subject
		}

		if sco.StreamFilterOptions.Properties.ReplyTo != nil {
			l["reply-to"] = *sco.StreamFilterOptions.Properties.ReplyTo
		}

		if sco.StreamFilterOptions.Properties.To != nil {
			l["to"] = *sco.StreamFilterOptions.Properties.To
		}

		if sco.StreamFilterOptions.Properties.GroupID != nil {
			l["group-id"] = *sco.StreamFilterOptions.Properties.GroupID
		}

		if sco.StreamFilterOptions.Properties.UserID != nil {
			l["user-id"] = sco.StreamFilterOptions.Properties.UserID
		}

		if sco.StreamFilterOptions.Properties.AbsoluteExpiryTime != nil {
			l["absolute-expiry-time"] = sco.StreamFilterOptions.Properties.AbsoluteExpiryTime
		}

		if sco.StreamFilterOptions.Properties.CreationTime != nil {
			l["creation-time"] = sco.StreamFilterOptions.Properties.CreationTime
		}

		if sco.StreamFilterOptions.Properties.GroupSequence != nil {
			l["group-sequence"] = *sco.StreamFilterOptions.Properties.GroupSequence
		}

		if sco.StreamFilterOptions.Properties.ReplyToGroupID != nil {
			l["reply-to-group-id"] = *sco.StreamFilterOptions.Properties.ReplyToGroupID
		}

		if len(l) > 0 {
			filters = append(filters, amqp.NewLinkFilter(amqpPropertiesFilter, 0, l))
		}
	}

	return filters
}

func (sco *StreamConsumerOptions) id() string {
	return sco.Id
}

func (sco *StreamConsumerOptions) validate(available *featuresAvailable) error {
	if sco.StreamFilterOptions != nil && sco.StreamFilterOptions.Properties != nil {
		if !available.is41OrMore {
			return fmt.Errorf("stream consumer with properties filter is not supported. You need RabbitMQ 4.1 or later")
		}
	}
	return nil
}

///// PublisherOptions /////

type IPublisherOptions interface {
	linkName() string
	id() string
}

type PublisherOptions struct {
	Id             string
	SenderLinkName string
}

func (apo *PublisherOptions) linkName() string {
	return apo.SenderLinkName
}

func (apo *PublisherOptions) id() string {
	return apo.Id
}
