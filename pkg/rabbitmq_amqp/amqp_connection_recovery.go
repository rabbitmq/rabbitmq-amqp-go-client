package rabbitmq_amqp

import (
	"sync"
	"time"
)

type RecoveryConfiguration struct {
	/*
		ActiveRecovery Define if the recovery is activated.
		If is not activated the connection will not try to createSender.
	*/
	ActiveRecovery bool

	/*
		BackOffReconnectInterval The time to wait before trying to createSender after a connection is closed.
		time will be increased exponentially with each attempt.
		Default is 5 seconds, each attempt will double the time.
		The minimum value is 1 second. Avoid setting a value low values since it can cause a high
		number of reconnection attempts.
	*/
	BackOffReconnectInterval time.Duration

	/*
		MaxReconnectAttempts The maximum number of reconnection attempts.
		Default is 5.
		The minimum value is 1.
	*/
	MaxReconnectAttempts int
}

func NewRecoveryConfiguration() *RecoveryConfiguration {
	return &RecoveryConfiguration{
		ActiveRecovery:           true,
		BackOffReconnectInterval: 5 * time.Second,
		MaxReconnectAttempts:     5,
	}
}

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

func (e *entitiesTracker) storeOrReplaceProducer(entity entityIdentifier) {
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

func (e *entitiesTracker) storeOrReplaceConsumer(entity entityIdentifier) {
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

func (e *entitiesTracker) CleanUp() {
	e.publishers.Range(func(key, value interface{}) bool {
		e.publishers.Delete(key)
		return true
	})
	e.consumers.Range(func(key, value interface{}) bool {
		e.consumers.Delete(key)
		return true
	})
}
