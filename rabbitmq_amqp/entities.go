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

// QueueSpecification represents the specification of a queue
type QueueSpecification struct {
	Name                 string
	IsAutoDelete         bool
	IsExclusive          bool
	QueueType            QueueType
	MaxLengthBytes       int64
	DeadLetterExchange   string
	DeadLetterRoutingKey string
}

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

type ExchangeSpecification struct {
	Name         string
	IsAutoDelete bool
	ExchangeType ExchangeType
}

type BindingSpecification struct {
	SourceExchange      string
	DestinationQueue    string
	DestinationExchange string
	BindingKey          string
}
