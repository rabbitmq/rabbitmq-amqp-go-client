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

func getLinkName(linkName string) string {
	if linkName == "" {
		return uuid.New().String()
	}
	return linkName
}

// ConsumerOptions interface for AMQP and Stream consumers
type ConsumerOptions interface {
	LinkName() string
	InitialCredits() int32
	LinkFilters() []amqp.LinkFilter
	ID() string
	Validate(available *featuresAvailable) error
}

func getInitialCredits(co ConsumerOptions) int32 {
	if co == nil {
		return 256
	}
	if credits := co.InitialCredits(); credits > 0 {
		return credits
	}
	return 256
}

func getLinkFilters(co ConsumerOptions) []amqp.LinkFilter {
	if co == nil {
		return nil
	}
	return co.LinkFilters()
}

type managementOptions struct {
}

func (mo *managementOptions) LinkName() string {
	return linkPairName
}

func (mo *managementOptions) InitialCredits() int32 {
	// by default i 256 but here we set it to 100. For the management is enough.
	return 100
}

func (mo *managementOptions) LinkFilters() []amqp.LinkFilter {
	return nil
}

func (mo *managementOptions) ID() string {
	return "management"
}

func (mo *managementOptions) Validate(available *featuresAvailable) error {
	return nil
}

// QueueConsumerOptions for quorum and classic queues
type QueueConsumerOptions struct {
	ReceiverLinkName string
	Credits          int32
	ConsumerID       string
}

func (aco *QueueConsumerOptions) LinkName() string {
	return aco.ReceiverLinkName
}

func (aco *QueueConsumerOptions) InitialCredits() int32 {
	return aco.Credits
}

func (aco *QueueConsumerOptions) LinkFilters() []amqp.LinkFilter {
	return nil
}

func (aco *QueueConsumerOptions) ID() string {
	return aco.ConsumerID
}

func (aco *QueueConsumerOptions) Validate(available *featuresAvailable) error {
	return nil
}

type OffsetSpecification interface {
	ToLinkFilter() amqp.LinkFilter
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

func (of *OffsetFirst) ToLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetFirst)
}

type OffsetLast struct {
}

func (ol *OffsetLast) ToLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetLast)
}

type OffsetValue struct {
	Offset uint64
}

func (ov *OffsetValue) ToLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, ov.Offset)
}

type OffsetNext struct {
}

func (on *OffsetNext) ToLinkFilter() amqp.LinkFilter {
	return amqp.NewLinkFilter(rmqStreamOffsetSpec, 0, offsetNext)
}

// StreamFilterOptions for filtering stream data
// See: https://www.rabbitmq.com/blog/2024/12/13/amqp-filter-expressions/
type StreamFilterOptions struct {
	Values                []string
	MatchUnfiltered       bool
	ApplicationProperties map[string]any
	Properties            *amqp.MessageProperties
}

// StreamConsumerOptions for stream queues
type StreamConsumerOptions struct {
	ReceiverLinkName    string
	Credits             int32
	Offset              OffsetSpecification
	StreamFilterOptions *StreamFilterOptions
	ConsumerID          string
}

func (sco *StreamConsumerOptions) LinkName() string {
	return sco.ReceiverLinkName
}

func (sco *StreamConsumerOptions) InitialCredits() int32 {
	return sco.Credits
}

func (sco *StreamConsumerOptions) LinkFilters() []amqp.LinkFilter {
	filters := []amqp.LinkFilter{sco.Offset.ToLinkFilter()}

	if sco.StreamFilterOptions == nil {
		return filters
	}

	opt := sco.StreamFilterOptions
	if len(opt.Values) > 0 {
		values := make([]any, len(opt.Values))
		for i, v := range opt.Values {
			values[i] = v
		}
		filters = append(filters,
			amqp.NewLinkFilter(rmqStreamFilter, 0, values),
			amqp.NewLinkFilter(rmqStreamMatchUnfiltered, 0, opt.MatchUnfiltered))
	}

	if len(opt.ApplicationProperties) > 0 {
		filters = append(filters, amqp.NewLinkFilter(amqpApplicationPropertiesFilter, 0, opt.ApplicationProperties))
	}

	if opt.Properties != nil {
		if propFilter := buildPropertiesFilter(opt.Properties); len(propFilter) > 0 {
			filters = append(filters, amqp.NewLinkFilter(amqpPropertiesFilter, 0, propFilter))
		}
	}

	return filters
}

func buildPropertiesFilter(props *amqp.MessageProperties) map[amqp.Symbol]any {
	filter := make(map[amqp.Symbol]any)

	if props.ContentType != nil {
		filter["content-type"] = amqp.Symbol(*props.ContentType)
	}
	if props.ContentEncoding != nil {
		filter["content-encoding"] = amqp.Symbol(*props.ContentEncoding)
	}
	if props.CorrelationID != nil {
		filter["correlation-id"] = props.CorrelationID
	}
	if props.MessageID != nil {
		filter["message-id"] = props.MessageID
	}
	if props.Subject != nil {
		filter["subject"] = *props.Subject
	}
	if props.ReplyTo != nil {
		filter["reply-to"] = *props.ReplyTo
	}
	if props.To != nil {
		filter["to"] = *props.To
	}
	if props.GroupID != nil {
		filter["group-id"] = *props.GroupID
	}
	if props.UserID != nil {
		filter["user-id"] = props.UserID
	}
	if props.AbsoluteExpiryTime != nil {
		filter["absolute-expiry-time"] = props.AbsoluteExpiryTime
	}
	if props.CreationTime != nil {
		filter["creation-time"] = props.CreationTime
	}
	if props.GroupSequence != nil {
		filter["group-sequence"] = *props.GroupSequence
	}
	if props.ReplyToGroupID != nil {
		filter["reply-to-group-id"] = *props.ReplyToGroupID
	}

	return filter
}

func (sco *StreamConsumerOptions) ID() string {
	return sco.ConsumerID
}

func (sco *StreamConsumerOptions) Validate(available *featuresAvailable) error {
	if sco.StreamFilterOptions != nil && sco.StreamFilterOptions.Properties != nil {
		if !available.is41OrMore {
			return fmt.Errorf("stream consumer with properties filter is not supported. You need RabbitMQ 4.1 or later")
		}
	}
	return nil
}

///// PublisherOptions /////

type PublisherOptions interface {
	LinkName() string
	ID() string
}

type QueuePublisherOptions struct {
	PublisherID    string
	SenderLinkName string
}

func (apo *QueuePublisherOptions) LinkName() string {
	return apo.SenderLinkName
}

func (apo *QueuePublisherOptions) ID() string {
	return apo.PublisherID
}
