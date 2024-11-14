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

// IQueueInfo represents the information of a queue
// It is returned by the Declare method of IQueueSpecification
// The information come from the server
type IQueueInfo interface {
	Name() string
	IsDurable() bool
	IsAutoDelete() bool
	IsExclusive() bool
	Type() TQueueType
	Leader() string
	Members() []string
	Arguments() map[string]any
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

// IExchangeInfo represents the information of an exchange
// It is empty at the moment because the server does not return any information
// We leave it here for future use. In case the server returns information about an exchange
type IExchangeInfo interface {
	Name() string
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
