package rabbitmqamqp

import (
	"errors"
	"sync"
	"time"
)

// ErrMaxReconnectAttemptsReached typed error when the MaxReconnectAttempts is reached
var ErrMaxReconnectAttemptsReached = errors.New("max reconnect attempts reached, connection will not be recovered")

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

func (c *RecoveryConfiguration) Clone() *RecoveryConfiguration {
	cloned := &RecoveryConfiguration{
		ActiveRecovery:           c.ActiveRecovery,
		BackOffReconnectInterval: c.BackOffReconnectInterval,
		MaxReconnectAttempts:     c.MaxReconnectAttempts,
	}

	return cloned

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

func (e *entitiesTracker) storeOrReplaceProducer(entity EntityIdentifier) {
	e.publishers.Store(entity.ID(), entity)
}

func (e *entitiesTracker) removeProducer(entity EntityIdentifier) {
	e.publishers.Delete(entity.ID())
}

func (e *entitiesTracker) storeOrReplaceConsumer(entity EntityIdentifier) {
	e.consumers.Store(entity.ID(), entity)
}

func (e *entitiesTracker) removeConsumer(entity EntityIdentifier) {
	e.consumers.Delete(entity.ID())
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
