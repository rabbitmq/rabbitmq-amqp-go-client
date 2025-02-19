package rabbitmqamqp

type entityIdentifier interface {
	Id() string
}

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

/*
QueueSpecification represents the specification of a queue
*/
type QueueSpecification interface {
	name() string
	isAutoDelete() bool
	isExclusive() bool
	queueType() QueueType
	buildArguments() map[string]any
}

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

/*
QuorumQueueSpecification represents the specification of the quorum queue
*/

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

/*
ClassicQueueSpecification represents the specification of the classic queue
*/
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

/*
AutoGeneratedQueueSpecification represents the specification of the auto-generated queue.
It is a classic queue with auto-generated name.
It is useful in context like RPC or when you need a temporary queue.
*/
type AutoGeneratedQueueSpecification struct {
	IsAutoDelete   bool
	IsExclusive    bool
	MaxLength      int64
	MaxLengthBytes int64
}

func (a *AutoGeneratedQueueSpecification) name() string {
	return generateNameWithDefaultPrefix()
}

func (a *AutoGeneratedQueueSpecification) isAutoDelete() bool {
	return a.IsAutoDelete
}

func (a *AutoGeneratedQueueSpecification) isExclusive() bool {
	return a.IsExclusive
}

func (a *AutoGeneratedQueueSpecification) queueType() QueueType {
	return QueueType{Classic}
}

func (a *AutoGeneratedQueueSpecification) buildArguments() map[string]any {
	result := map[string]any{}

	if a.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = a.MaxLengthBytes
	}

	if a.MaxLength != 0 {
		result["x-max-length"] = a.MaxLength
	}

	result["x-queue-type"] = a.queueType().String()

	return result
}

type StreamQueueSpecification struct {
	Name               string
	MaxLengthBytes     int64
	InitialClusterSize int
}

func (s *StreamQueueSpecification) name() string {
	return s.Name
}

func (s *StreamQueueSpecification) isAutoDelete() bool {
	return false
}

func (s *StreamQueueSpecification) isExclusive() bool {
	return false
}

func (s *StreamQueueSpecification) queueType() QueueType {
	return QueueType{Type: Stream}
}

func (s *StreamQueueSpecification) buildArguments() map[string]any {
	result := map[string]any{}

	if s.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = s.MaxLengthBytes
	}

	if s.InitialClusterSize != 0 {
		result["x-stream-initial-cluster-size"] = s.InitialClusterSize
	}

	result["x-queue-type"] = s.queueType().String()

	return result
}

// / **** Exchange ****

// TExchangeType represents the type of exchange
type TExchangeType string

const (
	Direct  TExchangeType = "direct"
	Topic   TExchangeType = "topic"
	FanOut  TExchangeType = "fanout"
	Headers TExchangeType = "headers"
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

type HeadersExchangeSpecification struct {
	Name         string
	IsAutoDelete bool
}

func (h *HeadersExchangeSpecification) name() string {
	return h.Name
}

func (h *HeadersExchangeSpecification) isAutoDelete() bool {
	return h.IsAutoDelete
}

func (h *HeadersExchangeSpecification) exchangeType() ExchangeType {
	return ExchangeType{Type: Headers}
}

func (h *HeadersExchangeSpecification) buildArguments() map[string]any {
	return map[string]any{}
}

type CustomExchangeSpecification struct {
	Name             string
	IsAutoDelete     bool
	ExchangeTypeName string
}

func (c *CustomExchangeSpecification) name() string {
	return c.Name
}

func (c *CustomExchangeSpecification) isAutoDelete() bool {
	return c.IsAutoDelete
}

func (c *CustomExchangeSpecification) exchangeType() ExchangeType {
	return ExchangeType{Type: TExchangeType(c.ExchangeTypeName)}
}

func (c *CustomExchangeSpecification) buildArguments() map[string]any {
	return map[string]any{}
}

// / **** Binding ****

type BindingSpecification interface {
	sourceExchange() string
	destination() string
	bindingKey() string
	isDestinationQueue() bool
}

type ExchangeToQueueBindingSpecification struct {
	SourceExchange   string
	DestinationQueue string
	BindingKey       string
}

func (e *ExchangeToQueueBindingSpecification) sourceExchange() string {
	return e.SourceExchange
}

func (e *ExchangeToQueueBindingSpecification) destination() string {
	return e.DestinationQueue
}

func (e *ExchangeToQueueBindingSpecification) isDestinationQueue() bool {
	return true
}

func (e *ExchangeToQueueBindingSpecification) bindingKey() string {
	return e.BindingKey
}

type ExchangeToExchangeBindingSpecification struct {
	SourceExchange      string
	DestinationExchange string
	BindingKey          string
}

func (e *ExchangeToExchangeBindingSpecification) sourceExchange() string {
	return e.SourceExchange
}

func (e *ExchangeToExchangeBindingSpecification) destination() string {
	return e.DestinationExchange
}

func (e *ExchangeToExchangeBindingSpecification) isDestinationQueue() bool {
	return false
}

func (e *ExchangeToExchangeBindingSpecification) bindingKey() string {
	return e.BindingKey
}
