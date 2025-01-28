package rabbitmq_amqp

import (
	"context"
	"github.com/Azure/go-amqp"
)

type AmqpExchangeInfo struct {
	name string
}

func newAmqpExchangeInfo(name string) *AmqpExchangeInfo {
	return &AmqpExchangeInfo{name: name}
}

func (a *AmqpExchangeInfo) Name() string {
	return a.name
}

type AmqpExchange struct {
	name         string
	management   *AmqpManagement
	arguments    map[string]any
	isAutoDelete bool
	exchangeType ExchangeType
}

func newAmqpExchange(management *AmqpManagement, name string) *AmqpExchange {
	return &AmqpExchange{management: management,
		name:         name,
		arguments:    make(map[string]any),
		exchangeType: ExchangeType{Type: Direct},
	}
}

func (e *AmqpExchange) Declare(ctx context.Context) (*AmqpExchangeInfo, error) {
	path, err := exchangeAddress(&e.name, nil)
	if err != nil {
		return nil, err
	}
	kv := make(map[string]any)
	kv["auto_delete"] = e.isAutoDelete
	kv["durable"] = true
	kv["type"] = e.exchangeType.String()
	kv["arguments"] = e.arguments
	_, err = e.management.Request(ctx, kv, path, commandPut, []int{responseCode204, responseCode201, responseCode409})
	if err != nil {
		return nil, err
	}
	return newAmqpExchangeInfo(e.name), nil
}

func (e *AmqpExchange) AutoDelete(isAutoDelete bool) {
	e.isAutoDelete = isAutoDelete
}

func (e *AmqpExchange) IsAutoDelete() bool {
	return e.isAutoDelete
}

func (e *AmqpExchange) Delete(ctx context.Context) error {
	path, err := exchangeAddress(&e.name, nil)
	if err != nil {
		return err
	}
	_, err = e.management.Request(ctx, amqp.Null{}, path, commandDelete, []int{responseCode204})
	return err
}

func (e *AmqpExchange) ExchangeType(exchangeType ExchangeType) {
	if len(exchangeType.Type) > 0 {
		e.exchangeType = exchangeType
	}
}

func (e *AmqpExchange) GetExchangeType() TExchangeType {
	return e.exchangeType.Type
}

func (e *AmqpExchange) Name() string {
	return e.name
}
