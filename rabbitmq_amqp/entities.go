package rabbitmq_amqp

type TQueueType string

const (
	Quorum  TQueueType = "quorum"
	Classic TQueueType = "classic"
	Stream  TQueueType = "stream"
)

type QueueType struct {
	Type TQueueType
}

func (e QueueType) String() string {
	return string(e.Type)
}

type QueueSpecification interface {
	name() string
	isAutoDelete() bool
	isExclusive() bool
	queueType() QueueType
	buildArguments() map[string]any
}

// QuorumQueueSpecification represents the specification of the quorum queue

type OverflowStrategy interface {
	overflowStrategy() string
}

type DropHeadOverflowStrategy struct {
}

func (d *DropHeadOverflowStrategy) overflowStrategy() string {
	return "drop-head"
}

type RejectPublishOverflowStrategy struct {
}

func (r *RejectPublishOverflowStrategy) overflowStrategy() string {
	return "reject-publish"
}

type RejectPublishDlxOverflowStrategy struct {
}

func (r *RejectPublishDlxOverflowStrategy) overflowStrategy() string {
	return "reject-publish-dlx"
}

type LeaderLocator interface {
	leaderLocator() string
}

type BalancedLeaderLocator struct {
}

func (r *BalancedLeaderLocator) leaderLocator() string {
	return "random"
}

type ClientLocalLeaderLocator struct {
}

func (r *ClientLocalLeaderLocator) leaderLocator() string {
	return "client-local"
}

type QuorumQueueSpecification struct {
	Name                 string
	AutoExpire           int64
	MessageTTL           int64
	OverflowStrategy     OverflowStrategy
	SingleActiveConsumer bool
	DeadLetterExchange   string
	DeadLetterRoutingKey string
	MaxLength            int64
	MaxLengthBytes       int64
	DeliveryLimit        int64
	TargetClusterSize    int64
	LeaderLocator        LeaderLocator
}

func (q *QuorumQueueSpecification) name() string {
	return q.Name
}

func (q *QuorumQueueSpecification) isAutoDelete() bool {
	return false
}

func (q *QuorumQueueSpecification) isExclusive() bool {
	return false
}

func (q *QuorumQueueSpecification) queueType() QueueType {
	return QueueType{Type: Quorum}
}

func (q *QuorumQueueSpecification) buildArguments() map[string]any {
	result := map[string]any{}
	if q.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = q.MaxLengthBytes
	}

	if len(q.DeadLetterExchange) != 0 {
		result["x-dead-letter-exchange"] = q.DeadLetterExchange
	}

	if len(q.DeadLetterRoutingKey) != 0 {
		result["x-dead-letter-routing-key"] = q.DeadLetterRoutingKey
	}

	if q.AutoExpire != 0 {
		result["x-expires"] = q.AutoExpire
	}

	if q.MessageTTL != 0 {
		result["x-message-ttl"] = q.MessageTTL
	}

	if q.OverflowStrategy != nil {
		result["x-overflow"] = q.OverflowStrategy.overflowStrategy()
	}

	if q.SingleActiveConsumer {
		result["x-single-active-consumer"] = true
	}

	if q.MaxLength != 0 {
		result["x-max-length"] = q.MaxLength
	}

	if q.DeliveryLimit != 0 {
		result["x-delivery-limit"] = q.DeliveryLimit
	}

	if q.TargetClusterSize != 0 {
		result["x-quorum-target-group-size"] = q.TargetClusterSize
	}

	if q.LeaderLocator != nil {
		result["x-queue-leader-locator"] = q.LeaderLocator.leaderLocator()
	}

	result["x-queue-type"] = q.queueType().String()
	return result
}

// ClassicQueueSpecification represents the specification of the classic queue
type ClassicQueueSpecification struct {
	Name                 string
	IsAutoDelete         bool
	IsExclusive          bool
	AutoExpire           int64
	MessageTTL           int64
	OverflowStrategy     OverflowStrategy
	SingleActiveConsumer bool
	DeadLetterExchange   string
	DeadLetterRoutingKey string
	MaxLength            int64
	MaxLengthBytes       int64
	MaxPriority          int64
	LeaderLocator        LeaderLocator
}

func (q *ClassicQueueSpecification) name() string {
	return q.Name
}

func (q *ClassicQueueSpecification) isAutoDelete() bool {
	return q.IsAutoDelete
}

func (q *ClassicQueueSpecification) isExclusive() bool {
	return q.IsExclusive
}

func (q *ClassicQueueSpecification) queueType() QueueType {
	return QueueType{Type: Classic}
}

func (q *ClassicQueueSpecification) buildArguments() map[string]any {
	result := map[string]any{}

	if q.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = q.MaxLengthBytes
	}

	if len(q.DeadLetterExchange) != 0 {
		result["x-dead-letter-exchange"] = q.DeadLetterExchange
	}

	if len(q.DeadLetterRoutingKey) != 0 {
		result["x-dead-letter-routing-key"] = q.DeadLetterRoutingKey
	}

	if q.AutoExpire != 0 {
		result["x-expires"] = q.AutoExpire
	}

	if q.MessageTTL != 0 {
		result["x-message-ttl"] = q.MessageTTL
	}

	if q.OverflowStrategy != nil {
		result["x-overflow"] = q.OverflowStrategy.overflowStrategy()
	}

	if q.SingleActiveConsumer {
		result["x-single-active-consumer"] = true
	}

	if q.MaxLength != 0 {
		result["x-max-length"] = q.MaxLength
	}

	if q.MaxPriority != 0 {
		result["x-max-priority"] = q.MaxPriority
	}

	if q.LeaderLocator != nil {
		result["x-queue-leader-locator"] = q.LeaderLocator.leaderLocator()
	}

	result["x-queue-type"] = q.queueType().String()

	return result
}

// / **** Exchange ****

// TExchangeType represents the type of exchange
type TExchangeType string

const (
	Direct TExchangeType = "direct"
	Topic  TExchangeType = "topic"
	FanOut TExchangeType = "fanout"
)

type ExchangeType struct {
	Type TExchangeType
}

func (e ExchangeType) String() string {
	return string(e.Type)
}

// ExchangeSpecification represents the specification of an exchange
type ExchangeSpecification interface {
	name() string
	isAutoDelete() bool
	exchangeType() ExchangeType
	buildArguments() map[string]any
}

type DirectExchangeSpecification struct {
	Name         string
	IsAutoDelete bool
}

func (d *DirectExchangeSpecification) name() string {
	return d.Name
}

func (d *DirectExchangeSpecification) isAutoDelete() bool {
	return d.IsAutoDelete
}

func (d *DirectExchangeSpecification) exchangeType() ExchangeType {
	return ExchangeType{Type: Direct}
}

func (d *DirectExchangeSpecification) buildArguments() map[string]any {
	return map[string]any{}
}

type TopicExchangeSpecification struct {
	Name         string
	IsAutoDelete bool
}

func (t *TopicExchangeSpecification) name() string {
	return t.Name
}

func (t *TopicExchangeSpecification) isAutoDelete() bool {
	return t.IsAutoDelete
}

func (t *TopicExchangeSpecification) exchangeType() ExchangeType {
	return ExchangeType{Type: Topic}
}

func (t *TopicExchangeSpecification) buildArguments() map[string]any {
	return map[string]any{}
}

type FanOutExchangeSpecification struct {
	Name         string
	IsAutoDelete bool
}

func (f *FanOutExchangeSpecification) name() string {
	return f.Name
}

func (f *FanOutExchangeSpecification) isAutoDelete() bool {
	return f.IsAutoDelete
}

func (f *FanOutExchangeSpecification) exchangeType() ExchangeType {
	return ExchangeType{Type: FanOut}
}

func (f *FanOutExchangeSpecification) buildArguments() map[string]any {
	return map[string]any{}
}

type BindingSpecification struct {
	SourceExchange      string
	DestinationQueue    string
	DestinationExchange string
	BindingKey          string
}
