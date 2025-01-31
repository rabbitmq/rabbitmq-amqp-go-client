package rabbitmq_amqp

import "sync"

type entitiesTracker struct {
	publishers sync.Map
	consumers  sync.Map
}

func newEntitiesTracker() *entitiesTracker {
	return &entitiesTracker{
		publishers: sync.Map{},
		consumers:  sync.Map{},
	}
}

func (e *entitiesTracker) addProducer(entity entityIdentifier) {
	e.publishers.Store(entity.Id(), entity)
}

func (e *entitiesTracker) getProducer(id string) (*Publisher, bool) {
	producer, ok := e.publishers.Load(id)
	if !ok {
		return nil, false
	}
	return producer.(*Publisher), true
}

func (e *entitiesTracker) removeProducer(entity entityIdentifier) {
	e.publishers.Delete(entity.Id())
}

func (e *entitiesTracker) addConsumer(entity entityIdentifier) {
	e.consumers.Store(entity.Id(), entity)
}

func (e *entitiesTracker) getConsumer(id string) (*Consumer, bool) {
	consumer, ok := e.consumers.Load(id)
	if !ok {
		return nil, false
	}
	return consumer.(*Consumer), true
}

func (e *entitiesTracker) removeConsumer(entity entityIdentifier) {
	e.consumers.Delete(entity.Id())
}
