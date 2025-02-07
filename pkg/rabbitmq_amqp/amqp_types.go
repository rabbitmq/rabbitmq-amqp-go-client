package rabbitmq_amqp

import "github.com/google/uuid"

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
}

func getInitialCredits(co ConsumerOptions) int32 {
	if co == nil {
		return 100
	}
	return co.initialCredits()
}

type managementOptions struct {
}

func (mo *managementOptions) linkName() string {
	return linkPairName
}

func (mo *managementOptions) initialCredits() int32 {
	return 10
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

type OffsetSpecification interface {
	toMap() map[string]any
}

type OffsetFirst struct {
}

func (of *OffsetFirst) toMap() map[string]any {
	return map[string]any{"offset": "first"}
}

type OffsetLast struct {
}

func (ol *OffsetLast) toMap() map[string]any {
	return map[string]any{"offset": "last"}
}

type OffsetValue struct {
	Offset uint64
}

func (o *OffsetValue) toMap() map[string]any {
	return map[string]any{"offset": o.Offset}
}

type StreamConsumerOptions struct {
	ReceiverLinkName string
	InitialCredits   uint32
	Offset           OffsetSpecification
}

func (sco *StreamConsumerOptions) linkName() string {
	return sco.ReceiverLinkName
}

func (sco *StreamConsumerOptions) initialCredits() uint32 {
	return sco.InitialCredits
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
