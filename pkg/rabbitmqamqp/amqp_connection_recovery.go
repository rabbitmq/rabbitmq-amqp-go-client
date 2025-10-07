package rabbitmqamqp

import (
	"errors"
	"slices"
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

func (e *entitiesTracker) storeOrReplaceProducer(entity iEntityIdentifier) {
	e.publishers.Store(entity.Id(), entity)
}

func (e *entitiesTracker) removeProducer(entity iEntityIdentifier) {
	e.publishers.Delete(entity.Id())
}

func (e *entitiesTracker) storeOrReplaceConsumer(entity iEntityIdentifier) {
	e.consumers.Store(entity.Id(), entity)
}

func (e *entitiesTracker) removeConsumer(entity iEntityIdentifier) {
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

type queueRecoveryRecord struct {
	queueName  string
	queueType  TQueueType
	autoDelete *bool
	exclusive  *bool
	arguments  map[string]any
}

func (q *queueRecoveryRecord) toIQueueSpecification() IQueueSpecification {
	switch q.queueType {
	case Quorum:
		return &QuorumQueueSpecification{
			Name:      q.queueName,
			Arguments: q.arguments,
		}
	case Classic:
		return &ClassicQueueSpecification{
			Name:         q.queueName,
			IsAutoDelete: *q.autoDelete,
			IsExclusive:  *q.exclusive,
			Arguments:    q.arguments,
		}
	case Stream:
		return &StreamQueueSpecification{
			Name:      q.queueName,
			Arguments: q.arguments,
		}
	}
	return nil
}

type exchangeRecoveryRecord struct {
	exchangeName string
	exchangeType TExchangeType
	autoDelete   bool
	arguments    map[string]any
}

func (e *exchangeRecoveryRecord) toIExchangeSpecification() IExchangeSpecification {
	switch e.exchangeType {
	case Direct:
		return &DirectExchangeSpecification{
			Name:         e.exchangeName,
			IsAutoDelete: e.autoDelete,
			Arguments:    e.arguments,
		}
	case Topic:
		return &TopicExchangeSpecification{
			Name:         e.exchangeName,
			IsAutoDelete: e.autoDelete,
			Arguments:    e.arguments,
		}
	case FanOut:
		return &FanOutExchangeSpecification{
			Name:         e.exchangeName,
			IsAutoDelete: e.autoDelete,
			Arguments:    e.arguments,
		}
	case Headers:
		return &HeadersExchangeSpecification{
			Name:         e.exchangeName,
			IsAutoDelete: e.autoDelete,
			Arguments:    e.arguments,
		}
	default:
		return &CustomExchangeSpecification{
			Name:             e.exchangeName,
			IsAutoDelete:     e.autoDelete,
			ExchangeTypeName: string(e.exchangeType),
			Arguments:        e.arguments,
		}
	}
}

type bindingRecoveryRecord struct {
	sourceExchange     string
	destination        string
	isDestinationQueue bool
	bindingKey         string
	arguments          map[string]any
	path               string
}

func (b *bindingRecoveryRecord) toIBindingSpecification() IBindingSpecification {
	if b.isDestinationQueue {
		return &ExchangeToQueueBindingSpecification{
			SourceExchange:   b.sourceExchange,
			DestinationQueue: b.destination,
			BindingKey:       b.bindingKey,
			Arguments:        b.arguments,
		}
	}
	return &ExchangeToExchangeBindingSpecification{
		SourceExchange:      b.sourceExchange,
		DestinationExchange: b.destination,
		BindingKey:          b.bindingKey,
		Arguments:           b.arguments,
	}
}

type topologyRecoveryRecords struct {
	queues    []queueRecoveryRecord
	exchanges []exchangeRecoveryRecord
	bindings  []bindingRecoveryRecord
}

func newTopologyRecoveryRecords() *topologyRecoveryRecords {
	return &topologyRecoveryRecords{
		queues:    make([]queueRecoveryRecord, 0),
		exchanges: make([]exchangeRecoveryRecord, 0),
		bindings:  make([]bindingRecoveryRecord, 0),
	}
}

func (t *topologyRecoveryRecords) addQueueRecord(record queueRecoveryRecord) {
	t.queues = append(t.queues, record)
}

func (t *topologyRecoveryRecords) removeQueueRecord(record queueRecoveryRecord) {
	t.queues = slices.DeleteFunc(t.queues, func(r queueRecoveryRecord) bool {
		return r.queueName == record.queueName
	})
}

func (t *topologyRecoveryRecords) addExchangeRecord(record exchangeRecoveryRecord) {
	t.exchanges = append(t.exchanges, record)
}

func (t *topologyRecoveryRecords) removeExchangeRecord(record exchangeRecoveryRecord) {
	t.exchanges = slices.DeleteFunc(t.exchanges, func(r exchangeRecoveryRecord) bool {
		return r.exchangeName == record.exchangeName
	})
}

func (t *topologyRecoveryRecords) addBindingRecord(record bindingRecoveryRecord) {
	t.bindings = append(t.bindings, record)
}

func (t *topologyRecoveryRecords) removeBindingRecord(bindingPath string) {
	t.bindings = slices.DeleteFunc(t.bindings, func(r bindingRecoveryRecord) bool {
		return r.path == bindingPath
	})
}

func (t *topologyRecoveryRecords) removeBindingRecordBySourceExchange(sourceExchange string) {
	t.bindings = slices.DeleteFunc(t.bindings, func(r bindingRecoveryRecord) bool {
		return r.sourceExchange == sourceExchange
	})
}

func (t *topologyRecoveryRecords) removeBindingRecordByDestinationQueue(destinationQueue string) {
	t.bindings = slices.DeleteFunc(t.bindings, func(r bindingRecoveryRecord) bool {
		return r.destination == destinationQueue
	})
}
