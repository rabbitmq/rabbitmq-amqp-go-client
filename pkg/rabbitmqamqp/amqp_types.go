package rabbitmqamqp

import (
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// the following types are alias to the go-amqp package

type DeliveryState = amqp.DeliveryState
type StateAccepted = amqp.StateAccepted
type StateRejected = amqp.StateRejected
type StateReleased = amqp.StateReleased
type StateModified = amqp.StateModified

type linkerName interface {
	linkName() string
}

func getLinkName(l linkerName) string {
	if l == nil || l.linkName() == "" {
		return uuid.New().String()
	}
	return l.linkName()
}

/// ConsumerOptions interface for the AMQP and Stream consumer///

type ConsumerOptions interface {
	// linkName returns the name of the link
	// if not set it will return a random UUID
	linkName() string
	// initialCredits returns the initial credits for the link
	// if not set it will return 256
	initialCredits() int32

	// linkFilters returns the link filters for the link.
	// It is mostly used for the stream consumers.
	linkFilters() []amqp.LinkFilter
}

func getInitialCredits(co ConsumerOptions) int32 {
	if co == nil || co.initialCredits() == 0 {
		return 256
	}
	return co.initialCredits()
}

func getLinkFilters(co ConsumerOptions) []amqp.LinkFilter {
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

type AMQPConsumerOptions struct {
	//ReceiverLinkName: see the ConsumerOptions interface
	ReceiverLinkName string
	//InitialCredits: see the ConsumerOptions interface
	InitialCredits int32
}

func (aco *AMQPConsumerOptions) linkName() string {
	return aco.ReceiverLinkName
}

func (aco *AMQPConsumerOptions) initialCredits() int32 {
	return aco.InitialCredits
}

func (aco *AMQPConsumerOptions) linkFilters() []amqp.LinkFilter {
	return nil
}

type OffsetSpecification interface {
	toLinkFilter() amqp.LinkFilter
}

const rmqStreamFilter = "rabbitmq:stream-filter"
const rmqStreamOffsetSpec = "rabbitmq:stream-offset-spec"
const rmqStreamMatchUnfiltered = "rabbitmq:stream-match-unfiltered"
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

/*
StreamConsumerOptions represents the options that can be used to create a stream consumer.
It is mandatory in case of creating a stream consumer.
*/
type StreamConsumerOptions struct {
	//ReceiverLinkName: see the ConsumerOptions interface
	ReceiverLinkName string
	//InitialCredits: see the ConsumerOptions interface
	InitialCredits int32
	// The offset specification for the stream consumer
	// see the interface implementations
	Offset OffsetSpecification
	// Filter values.
	// See: https://www.rabbitmq.com/blog/2024/12/13/amqp-filter-expressions for more details
	Filters []string
	//
	FilterMatchUnfiltered bool
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
	if sco.Filters != nil {
		l := []any{}
		for _, f := range sco.Filters {
			l = append(l, f)
		}

		filters = append(filters, amqp.NewLinkFilter(rmqStreamFilter, 0, l))
		filters = append(filters, amqp.NewLinkFilter(rmqStreamMatchUnfiltered, 0, sco.FilterMatchUnfiltered))
	}
	return filters
}

///// ProducerOptions /////

type ProducerOptions interface {
	linkName() string
}

type AMQPProducerOptions struct {
	SenderLinkName string
}

func (apo *AMQPProducerOptions) linkName() string {
	return apo.SenderLinkName
}
