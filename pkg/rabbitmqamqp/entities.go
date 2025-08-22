package rabbitmqamqp

type EntityIdentifier interface {
	ID() string
}

type QueueType string

const (
	Quorum  QueueType = "quorum"
	Classic QueueType = "classic"
	Stream  QueueType = "stream"
)

func (e QueueType) String() string {
	return string(e)
}

// QueueSpecification defines queue properties
type QueueSpecification interface {
	Name() string
	IsAutoDelete() bool
	IsExclusive() bool
	QueueType() QueueType
	BuildArguments() map[string]any
}

type OverflowStrategy interface {
	OverflowStrategy() string
}

type DropHeadOverflowStrategy struct {
}

func (d *DropHeadOverflowStrategy) OverflowStrategy() string {
	return "drop-head"
}

type RejectPublishOverflowStrategy struct {
}

func (r *RejectPublishOverflowStrategy) OverflowStrategy() string {
	return "reject-publish"
}

type RejectPublishDlxOverflowStrategy struct {
}

func (r *RejectPublishDlxOverflowStrategy) OverflowStrategy() string {
	return "reject-publish-dlx"
}

type LeaderLocator interface {
	LeaderLocator() string
}

type BalancedLeaderLocator struct {
}

func (r *BalancedLeaderLocator) LeaderLocator() string {
	return "random"
}

type ClientLocalLeaderLocator struct {
}

func (r *ClientLocalLeaderLocator) LeaderLocator() string {
	return "client-local"
}

// QuorumQueueSpecification for quorum queues

type QuorumQueueSpecification struct {
	QueueName              string
	AutoExpire             int64
	MessageTTL             int64
	OverflowStrategy       OverflowStrategy
	SingleActiveConsumer   bool
	DeadLetterExchange     string
	DeadLetterRoutingKey   string
	MaxLength              int64
	MaxLengthBytes         int64
	DeliveryLimit          int64
	TargetClusterSize      int64
	LeaderLocator          LeaderLocator
	QuorumInitialGroupSize int
	Arguments              map[string]any
}

func (q *QuorumQueueSpecification) Name() string {
	return q.QueueName
}

func (q *QuorumQueueSpecification) IsAutoDelete() bool {
	return false
}

func (q *QuorumQueueSpecification) IsExclusive() bool {
	return false
}

func (q *QuorumQueueSpecification) QueueType() QueueType {
	return Quorum
}

func (q *QuorumQueueSpecification) BuildArguments() map[string]any {
	result := q.Arguments
	if result == nil {
		result = map[string]any{}
	}

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
		result["x-overflow"] = q.OverflowStrategy.OverflowStrategy()
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
		result["x-queue-leader-locator"] = q.LeaderLocator.LeaderLocator()
	}

	if q.QuorumInitialGroupSize != 0 {
		result["x-quorum-initial-group-size"] = q.QuorumInitialGroupSize
	}

	result["x-queue-type"] = q.QueueType().String()
	return result
}

// ClassicQueueSpecification for classic queues
type ClassicQueueSpecification struct {
	QueueName            string
	AutoDelete           bool
	Exclusive            bool
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
	Arguments            map[string]any
}

func (q *ClassicQueueSpecification) Name() string {
	return q.QueueName
}

func (q *ClassicQueueSpecification) IsAutoDelete() bool {
	return q.AutoDelete
}

func (q *ClassicQueueSpecification) IsExclusive() bool {
	return q.Exclusive
}

func (q *ClassicQueueSpecification) QueueType() QueueType {
	return Classic
}

func (q *ClassicQueueSpecification) BuildArguments() map[string]any {
	result := q.Arguments
	if result == nil {
		result = map[string]any{}
	}

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
		result["x-overflow"] = q.OverflowStrategy.OverflowStrategy()
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
		result["x-queue-leader-locator"] = q.LeaderLocator.LeaderLocator()
	}

	result["x-queue-type"] = q.QueueType().String()

	return result
}

// AutoGeneratedQueueSpecification for temporary queues with auto-generated names
type AutoGeneratedQueueSpecification struct {
	AutoDelete     bool
	Exclusive      bool
	MaxLength      int64
	MaxLengthBytes int64
	Arguments      map[string]any
}

func (a *AutoGeneratedQueueSpecification) Name() string {
	return generateNameWithDefaultPrefix()
}

func (a *AutoGeneratedQueueSpecification) IsAutoDelete() bool {
	return a.AutoDelete
}

func (a *AutoGeneratedQueueSpecification) IsExclusive() bool {
	return a.Exclusive
}

func (a *AutoGeneratedQueueSpecification) QueueType() QueueType {
	return Classic
}

func (a *AutoGeneratedQueueSpecification) BuildArguments() map[string]any {
	result := a.Arguments
	if result == nil {
		result = map[string]any{}
	}

	if a.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = a.MaxLengthBytes
	}

	if a.MaxLength != 0 {
		result["x-max-length"] = a.MaxLength
	}

	result["x-queue-type"] = a.QueueType().String()

	return result
}

type StreamQueueSpecification struct {
	QueueName          string
	MaxLengthBytes     int64
	InitialClusterSize int
	Arguments          map[string]any
}

func (s *StreamQueueSpecification) Name() string {
	return s.QueueName
}

func (s *StreamQueueSpecification) IsAutoDelete() bool {
	return false
}

func (s *StreamQueueSpecification) IsExclusive() bool {
	return false
}

func (s *StreamQueueSpecification) QueueType() QueueType {
	return Stream
}

func (s *StreamQueueSpecification) BuildArguments() map[string]any {
	result := s.Arguments
	if result == nil {
		result = map[string]any{}
	}

	if s.MaxLengthBytes != 0 {
		result["x-max-length-bytes"] = s.MaxLengthBytes
	}

	if s.InitialClusterSize != 0 {
		result["x-stream-initial-cluster-size"] = s.InitialClusterSize
	}

	result["x-queue-type"] = s.QueueType().String()

	return result
}

// ExchangeType represents the type of exchange
type ExchangeType string

const (
	Direct  ExchangeType = "direct"
	Topic   ExchangeType = "topic"
	FanOut  ExchangeType = "fanout"
	Headers ExchangeType = "headers"
)

func (e ExchangeType) String() string {
	return string(e)
}

// ExchangeSpecification defines exchange properties
type ExchangeSpecification interface {
	Name() string
	IsAutoDelete() bool
	ExchangeType() ExchangeType
	Arguments() map[string]any
}

type DirectExchangeSpecification struct {
	ExchangeName string
	AutoDelete   bool
	Args         map[string]any
}

func (d *DirectExchangeSpecification) Name() string {
	return d.ExchangeName
}

func (d *DirectExchangeSpecification) IsAutoDelete() bool {
	return d.AutoDelete
}

func (d *DirectExchangeSpecification) ExchangeType() ExchangeType {
	return Direct
}

func (d *DirectExchangeSpecification) Arguments() map[string]any {
	return d.Args
}

type TopicExchangeSpecification struct {
	ExchangeName string
	AutoDelete   bool
	Args         map[string]any
}

func (t *TopicExchangeSpecification) Name() string {
	return t.ExchangeName
}

func (t *TopicExchangeSpecification) IsAutoDelete() bool {
	return t.AutoDelete
}

func (t *TopicExchangeSpecification) ExchangeType() ExchangeType {
	return Topic
}

func (t *TopicExchangeSpecification) Arguments() map[string]any {
	return t.Args
}

type FanOutExchangeSpecification struct {
	ExchangeName string
	AutoDelete   bool
	Args         map[string]any
}

func (f *FanOutExchangeSpecification) Name() string {
	return f.ExchangeName
}

func (f *FanOutExchangeSpecification) IsAutoDelete() bool {
	return f.AutoDelete
}

func (f *FanOutExchangeSpecification) ExchangeType() ExchangeType {
	return FanOut
}

func (f *FanOutExchangeSpecification) Arguments() map[string]any {
	return f.Args
}

type HeadersExchangeSpecification struct {
	ExchangeName string
	AutoDelete   bool
	Args         map[string]any
}

func (h *HeadersExchangeSpecification) Name() string {
	return h.ExchangeName
}

func (h *HeadersExchangeSpecification) IsAutoDelete() bool {
	return h.AutoDelete
}

func (h *HeadersExchangeSpecification) ExchangeType() ExchangeType {
	return Headers
}

func (h *HeadersExchangeSpecification) Arguments() map[string]any {
	return h.Args
}

type CustomExchangeSpecification struct {
	ExchangeName     string
	AutoDelete       bool
	ExchangeTypeName string
	Args             map[string]any
}

func (c *CustomExchangeSpecification) Name() string {
	return c.ExchangeName
}

func (c *CustomExchangeSpecification) IsAutoDelete() bool {
	return c.AutoDelete
}

func (c *CustomExchangeSpecification) ExchangeType() ExchangeType {
	return ExchangeType(c.ExchangeTypeName)
}

func (c *CustomExchangeSpecification) Arguments() map[string]any {
	return c.Args
}

type BindingSpecification interface {
	SourceExchange() string
	Destination() string
	BindingKey() string
	IsDestinationQueue() bool
	Arguments() map[string]any
}

type ExchangeToQueueBindingSpecification struct {
	Source string
	Queue  string
	Key    string
	Args   map[string]any
}

func (e *ExchangeToQueueBindingSpecification) SourceExchange() string {
	return e.Source
}

func (e *ExchangeToQueueBindingSpecification) Destination() string {
	return e.Queue
}

func (e *ExchangeToQueueBindingSpecification) IsDestinationQueue() bool {
	return true
}

func (e *ExchangeToQueueBindingSpecification) BindingKey() string {
	return e.Key
}

func (e *ExchangeToQueueBindingSpecification) Arguments() map[string]any {
	return e.Args
}

type ExchangeToExchangeBindingSpecification struct {
	Source   string
	Exchange string
	Key      string
	Args     map[string]any
}

func (e *ExchangeToExchangeBindingSpecification) SourceExchange() string {
	return e.Source
}

func (e *ExchangeToExchangeBindingSpecification) Destination() string {
	return e.Exchange
}

func (e *ExchangeToExchangeBindingSpecification) IsDestinationQueue() bool {
	return false
}

func (e *ExchangeToExchangeBindingSpecification) BindingKey() string {
	return e.Key
}

func (e *ExchangeToExchangeBindingSpecification) Arguments() map[string]any {
	return e.Args
}
