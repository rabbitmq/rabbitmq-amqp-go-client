package rabbitmq_amqp

import (
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

type linkerName interface {
	linkName() string
}

func getLinkName(l linkerName) string {
	if l == nil {
		return uuid.New().String()
	}
	return l.linkName()
}

/// ConsumerOptions ///

type ConsumerOptions interface {
	linkName() string
	initialCredits() int32
	linkFilters() []amqp.LinkFilter
}

func getInitialCredits(co ConsumerOptions) int32 {
	if co == nil || co.initialCredits() == 0 {
		return 10
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
	return 10
}

func (mo *managementOptions) linkFilters() []amqp.LinkFilter {
	return nil
}

type AMQPConsumerOptions struct {
	ReceiverLinkName string
	InitialCredits   uint32
}

func (aco *AMQPConsumerOptions) linkName() string {
	return aco.ReceiverLinkName
}

func (aco *AMQPConsumerOptions) initialCredits() uint32 {
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

type StreamConsumerOptions struct {
	ReceiverLinkName string
	InitialCredits   int32
	Offset           OffsetSpecification
}

func (sco *StreamConsumerOptions) linkName() string {
	return sco.ReceiverLinkName
}

func (sco *StreamConsumerOptions) initialCredits() int32 {
	return sco.InitialCredits
}

func (sco *StreamConsumerOptions) linkFilters() []amqp.LinkFilter {
	return []amqp.LinkFilter{sco.Offset.toLinkFilter()}
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
